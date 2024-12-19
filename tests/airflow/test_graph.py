import os
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow import __version__ as airflow_version
from airflow.models import DAG
from airflow.models.abstractoperator import DEFAULT_OWNER
from airflow.utils.task_group import TaskGroup
from packaging import version

from cosmos.airflow.graph import (
    _snake_case_to_camelcase,
    build_airflow_graph,
    calculate_leaves,
    calculate_operator_class,
    create_task_metadata,
    create_test_task_metadata,
    generate_task_or_group,
)
from cosmos.config import ProfileConfig, RenderConfig
from cosmos.constants import (
    DBT_COMPILE_TASK_ID,
    DbtResourceType,
    ExecutionMode,
    SourceRenderingBehavior,
    TestBehavior,
    TestIndirectSelection,
)
from cosmos.converter import airflow_kwargs
from cosmos.dbt.graph import DbtNode
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping, PostgresUserPasswordProfileMapping

SAMPLE_PROJ_PATH = Path("/home/user/path/dbt-proj/")
SOURCE_RENDERING_BEHAVIOR = SourceRenderingBehavior(os.getenv("SOURCE_RENDERING_BEHAVIOR", "none"))

parent_seed = DbtNode(
    unique_id=f"{DbtResourceType.SEED.value}.{SAMPLE_PROJ_PATH.stem}.seed_parent",
    resource_type=DbtResourceType.SEED,
    depends_on=[],
    file_path="",
)
parent_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.parent",
    resource_type=DbtResourceType.MODEL,
    depends_on=[parent_seed.unique_id],
    file_path=SAMPLE_PROJ_PATH / "gen2/models/parent.sql",
    tags=["has_child"],
    config={"materialized": "view", "meta": {"owner": "parent_node"}},
    has_test=True,
)
test_parent_node = DbtNode(
    unique_id=f"{DbtResourceType.TEST.value}.{SAMPLE_PROJ_PATH.stem}.test_parent",
    resource_type=DbtResourceType.TEST,
    depends_on=[parent_node.unique_id],
    file_path="",
)
child_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.child",
    resource_type=DbtResourceType.MODEL,
    depends_on=[parent_node.unique_id],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/child.sql",
    tags=["nightly"],
    config={"materialized": "table", "meta": {"cosmos": {"operator_kwargs": {"queue": "custom_queue"}}}},
)

child2_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.child2.v2",
    resource_type=DbtResourceType.MODEL,
    depends_on=[parent_node.unique_id],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/child2_v2.sql",
    tags=["nightly"],
    config={"materialized": "table", "meta": {"cosmos": {"operator_kwargs": {"pool": "custom_pool"}}}},
)

sample_nodes_list = [parent_seed, parent_node, test_parent_node, child_node, child2_node]
sample_nodes = {node.unique_id: node for node in sample_nodes_list}


@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.4"),
    reason="Airflow DAG did not have task_group_dict until the 2.4 release",
)
@pytest.mark.integration
def test_build_airflow_graph_with_after_each():
    with DAG("test-id", start_date=datetime(2022, 1, 1)) as dag:
        task_args = {
            "project_dir": SAMPLE_PROJ_PATH,
            "conn_id": "fake_conn",
            "profile_config": ProfileConfig(
                profile_name="default",
                target_name="default",
                profile_mapping=PostgresUserPasswordProfileMapping(
                    conn_id="fake_conn",
                    profile_args={"schema": "public"},
                ),
            ),
        }
        build_airflow_graph(
            nodes=sample_nodes,
            dag=dag,
            execution_mode=ExecutionMode.LOCAL,
            test_indirect_selection=TestIndirectSelection.EAGER,
            task_args=task_args,
            render_config=RenderConfig(
                test_behavior=TestBehavior.AFTER_EACH,
                source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
            ),
            dbt_project_name="astro_shop",
        )
    topological_sort = [task.task_id for task in dag.topological_sort()]
    expected_sort = [
        "seed_parent_seed",
        "parent.run",
        "parent.test",
        "child_run",
        "child2_v2_run",
    ]

    assert topological_sort == expected_sort
    task_groups = dag.task_group_dict
    assert len(task_groups) == 1

    assert task_groups["parent"].upstream_task_ids == {"seed_parent_seed"}
    assert list(task_groups["parent"].children.keys()) == ["parent.run", "parent.test"]

    assert len(dag.leaves) == 2
    assert dag.leaves[0].task_id == "child_run"
    assert dag.leaves[1].task_id == "child2_v2_run"

    task_seed_parent_seed = dag.tasks[0]
    task_parent_run = dag.tasks[1]

    assert task_seed_parent_seed.owner == DEFAULT_OWNER
    assert task_parent_run.owner == "parent_node"
    assert {d for d in dag.owner.split(", ")} == {DEFAULT_OWNER, "parent_node"}


@pytest.mark.parametrize(
    "node_type,task_suffix",
    [(DbtResourceType.MODEL, "run"), (DbtResourceType.SEED, "seed"), (DbtResourceType.SNAPSHOT, "snapshot")],
)
def test_create_task_group_for_after_each_supported_nodes(node_type: DbtResourceType, task_suffix):
    """
    dbt test runs tests defined on models, sources, snapshots, and seeds.
    It expects that you have already created those resources through the appropriate commands.
    https://docs.getdbt.com/reference/commands/test
    """
    with DAG("test-task-group-after-each", start_date=datetime(2022, 1, 1)) as dag:
        node = DbtNode(
            unique_id=f"{node_type.value}.{SAMPLE_PROJ_PATH.stem}.dbt_node",
            resource_type=node_type,
            file_path=SAMPLE_PROJ_PATH / "gen2/models/parent.sql",
            tags=["has_child"],
            config={"materialized": "view"},
            depends_on=[],
            has_test=True,
        )
    output = generate_task_or_group(
        dag=dag,
        task_group=None,
        node=node,
        execution_mode=ExecutionMode.LOCAL,
        test_indirect_selection=TestIndirectSelection.EAGER,
        task_args={
            "project_dir": SAMPLE_PROJ_PATH,
            "profile_config": ProfileConfig(
                profile_name="default",
                target_name="default",
                profile_mapping=PostgresUserPasswordProfileMapping(
                    conn_id="fake_conn",
                    profile_args={"schema": "public"},
                ),
            ),
        },
        test_behavior=TestBehavior.AFTER_EACH,
        on_warning_callback=None,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    assert isinstance(output, TaskGroup)
    assert list(output.children.keys()) == [f"dbt_node.{task_suffix}", "dbt_node.test"]


@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.4"),
    reason="Airflow DAG did not have task_group_dict until the 2.4 release",
)
@pytest.mark.integration
def test_build_airflow_graph_with_after_all():
    with DAG("test-id", start_date=datetime(2022, 1, 1)) as dag:
        task_args = {
            "project_dir": SAMPLE_PROJ_PATH,
            "conn_id": "fake_conn",
            "profile_config": ProfileConfig(
                profile_name="default",
                target_name="default",
                profile_mapping=PostgresUserPasswordProfileMapping(
                    conn_id="fake_conn",
                    profile_args={"schema": "public"},
                ),
            ),
        }
        render_config = RenderConfig(
            select=["tag:some"],
            test_behavior=TestBehavior.AFTER_ALL,
            source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
        )
        build_airflow_graph(
            nodes=sample_nodes,
            dag=dag,
            execution_mode=ExecutionMode.LOCAL,
            test_indirect_selection=TestIndirectSelection.EAGER,
            task_args=task_args,
            dbt_project_name="astro_shop",
            render_config=render_config,
        )
    topological_sort = [task.task_id for task in dag.topological_sort()]
    expected_sort = ["seed_parent_seed", "parent_run", "child_run", "child2_v2_run", "astro_shop_test"]
    assert topological_sort == expected_sort

    task_groups = dag.task_group_dict
    assert len(task_groups) == 0

    assert len(dag.leaves) == 1
    assert dag.leaves[0].task_id == "astro_shop_test"
    assert dag.leaves[0].select == ["tag:some"]


@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.4"),
    reason="Airflow DAG did not have task_group_dict until the 2.4 release",
)
@pytest.mark.integration
def test_build_airflow_graph_with_build():
    with DAG("test-id", start_date=datetime(2022, 1, 1)) as dag:
        task_args = {
            "project_dir": SAMPLE_PROJ_PATH,
            "conn_id": "fake_conn",
            "profile_config": ProfileConfig(
                profile_name="default",
                target_name="default",
                profile_mapping=PostgresUserPasswordProfileMapping(
                    conn_id="fake_conn",
                    profile_args={"schema": "public"},
                ),
            ),
        }
        render_config = RenderConfig(
            test_behavior=TestBehavior.BUILD,
        )
        build_airflow_graph(
            nodes=sample_nodes,
            dag=dag,
            execution_mode=ExecutionMode.LOCAL,
            test_indirect_selection=TestIndirectSelection.EAGER,
            task_args=task_args,
            dbt_project_name="astro_shop",
            render_config=render_config,
        )
    topological_sort = [task.task_id for task in dag.topological_sort()]
    expected_sort = ["seed_parent_build", "parent_build", "child_build", "child2_v2_build"]
    assert topological_sort == expected_sort

    task_groups = dag.task_group_dict
    assert len(task_groups) == 0

    assert len(dag.leaves) == 2
    assert dag.leaves[0].task_id in ("child_build", "child2_v2_build")
    assert dag.leaves[1].task_id in ("child_build", "child2_v2_build")


@pytest.mark.integration
@patch("airflow.hooks.base.BaseHook.get_connection", new=MagicMock())
def test_build_airflow_graph_with_dbt_compile_task():
    bigquery_profile_config = ProfileConfig(
        profile_name="my-bigquery-db",
        target_name="dev",
        profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
            conn_id="fake_conn", profile_args={"dataset": "release_17"}
        ),
    )
    with DAG("test-id-dbt-compile", start_date=datetime(2022, 1, 1)) as dag:
        task_args = {
            "project_dir": SAMPLE_PROJ_PATH,
            "conn_id": "fake_conn",
            "profile_config": bigquery_profile_config,
            "location": "",
        }
        render_config = RenderConfig(
            select=["tag:some"],
            test_behavior=TestBehavior.AFTER_ALL,
            source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
        )
        build_airflow_graph(
            nodes=sample_nodes,
            dag=dag,
            execution_mode=ExecutionMode.AIRFLOW_ASYNC,
            test_indirect_selection=TestIndirectSelection.EAGER,
            task_args=task_args,
            dbt_project_name="astro_shop",
            render_config=render_config,
        )

    task_ids = [task.task_id for task in dag.tasks]
    assert DBT_COMPILE_TASK_ID in task_ids
    assert DBT_COMPILE_TASK_ID in dag.tasks[0].upstream_task_ids


def test_calculate_operator_class():
    class_module_import_path = calculate_operator_class(execution_mode=ExecutionMode.KUBERNETES, dbt_class="DbtSeed")
    assert class_module_import_path == "cosmos.operators.kubernetes.DbtSeedKubernetesOperator"


def test_calculate_leaves():
    grandparent_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.grandparent",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    parent1_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.parent1",
        resource_type=DbtResourceType.MODEL,
        depends_on=[grandparent_node.unique_id],
        file_path="",
        tags=[],
        config={},
    )
    parent2_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.parent2",
        resource_type=DbtResourceType.MODEL,
        depends_on=[parent1_node.unique_id],
        file_path="",
        tags=[],
        config={},
    )
    child_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.child",
        resource_type=DbtResourceType.MODEL,
        depends_on=[parent1_node.unique_id, parent2_node.unique_id],
        file_path="",
        tags=[],
        config={},
    )

    nodes_list = [grandparent_node, parent1_node, parent2_node, child_node]
    nodes = {node.unique_id: node for node in nodes_list}

    leaves = calculate_leaves(nodes.keys(), nodes)
    assert leaves == [f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.child"]


@patch("cosmos.airflow.graph.logger.propagate", True)
def test_create_task_metadata_unsupported(caplog):
    child_node = DbtNode(
        unique_id=f"unsupported.{SAMPLE_PROJ_PATH.stem}.unsupported",
        resource_type="unsupported",
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    response = create_task_metadata(child_node, execution_mode="", args={}, dbt_dag_task_group_identifier="")
    assert response is None
    expected_msg = (
        "Unavailable conversion function for <unsupported> (node <unsupported.dbt-proj.unsupported>). "
        "Define a converter function using render_config.node_converters."
    )
    assert caplog.messages[0] == expected_msg


@pytest.mark.parametrize(
    "unique_id, resource_type, expected_id, expected_operator_class, expected_arguments, expected_extra_context",
    [
        (
            f"{DbtResourceType.MODEL.value}.my_folder.my_model",
            DbtResourceType.MODEL,
            "my_model_run",
            "cosmos.operators.local.DbtRunLocalOperator",
            {"models": "my_model"},
            {
                "dbt_dag_task_group_identifier": "",
                "dbt_node_config": {
                    "unique_id": "model.my_folder.my_model",
                    "resource_type": "model",
                    "depends_on": [],
                    "file_path": ".",
                    "tags": [],
                    "config": {},
                    "has_test": False,
                    "resource_name": "my_model",
                    "name": "my_model",
                },
            },
        ),
        (
            f"{DbtResourceType.SOURCE.value}.my_folder.my_source",
            DbtResourceType.SOURCE,
            "my_source_source",
            "cosmos.operators.local.DbtSourceLocalOperator",
            {"select": "source:my_source"},
            {
                "dbt_node_config": {
                    "unique_id": "source.my_folder.my_source",
                    "resource_type": "source",
                    "depends_on": [],
                    "file_path": ".",
                    "tags": [],
                    "config": {},
                    "has_test": False,
                    "resource_name": "my_source",
                    "name": "my_source",
                }
            },
        ),
        (
            f"{DbtResourceType.SNAPSHOT.value}.my_folder.my_snapshot",
            DbtResourceType.SNAPSHOT,
            "my_snapshot_snapshot",
            "cosmos.operators.local.DbtSnapshotLocalOperator",
            {"models": "my_snapshot"},
            {
                "dbt_dag_task_group_identifier": "",
                "dbt_node_config": {
                    "unique_id": "snapshot.my_folder.my_snapshot",
                    "resource_type": "snapshot",
                    "depends_on": [],
                    "file_path": ".",
                    "tags": [],
                    "config": {},
                    "has_test": False,
                    "resource_name": "my_snapshot",
                    "name": "my_snapshot",
                },
            },
        ),
    ],
)
def test_create_task_metadata_model(
    unique_id,
    resource_type,
    expected_id,
    expected_operator_class,
    expected_arguments,
    expected_extra_context,
    caplog,
):
    child_node = DbtNode(
        unique_id=unique_id,
        resource_type=resource_type,
        depends_on=[],
        file_path=Path(""),
        tags=[],
        config={},
        has_freshness=True,
    )

    metadata = create_task_metadata(
        child_node, execution_mode=ExecutionMode.LOCAL, args={}, dbt_dag_task_group_identifier=""
    )
    if metadata:
        assert metadata.id == expected_id
        assert metadata.operator_class == expected_operator_class
        assert metadata.arguments == expected_arguments
        assert metadata.extra_context == expected_extra_context


def test_create_task_metadata_model_with_versions(caplog):
    child_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.my_folder.my_model.v1",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    metadata = create_task_metadata(
        child_node, execution_mode=ExecutionMode.LOCAL, args={}, dbt_dag_task_group_identifier=""
    )
    assert metadata.id == "my_model_v1_run"
    assert metadata.operator_class == "cosmos.operators.local.DbtRunLocalOperator"
    assert metadata.arguments == {"models": "my_model.v1"}


def test_create_task_metadata_model_use_task_group(caplog):
    child_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.my_folder.my_model",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path=Path(""),
        tags=[],
        config={},
    )
    metadata = create_task_metadata(
        child_node, execution_mode=ExecutionMode.LOCAL, args={}, use_task_group=True, dbt_dag_task_group_identifier=""
    )
    assert metadata.id == "run"


@pytest.mark.parametrize(
    "unique_id, resource_type, has_freshness, source_rendering_behavior, expected_id, expected_operator_class",
    [
        (
            f"{DbtResourceType.SOURCE.value}.my_folder.my_source",
            DbtResourceType.SOURCE,
            True,
            SOURCE_RENDERING_BEHAVIOR,
            "my_source_source",
            "cosmos.operators.local.DbtSourceLocalOperator",
        ),
        (
            f"{DbtResourceType.SOURCE.value}.my_folder.my_source",
            DbtResourceType.SOURCE,
            False,
            SOURCE_RENDERING_BEHAVIOR,
            "my_source_source",
            "airflow.operators.empty.EmptyOperator",
        ),
        (
            f"{DbtResourceType.SOURCE.value}.my_folder.my_source",
            DbtResourceType.SOURCE,
            True,
            SourceRenderingBehavior.NONE,
            None,
            None,
        ),
        (
            f"{DbtResourceType.SOURCE.value}.my_folder.my_source",
            DbtResourceType.SOURCE,
            False,
            SourceRenderingBehavior.NONE,
            None,
            None,
        ),
    ],
)
def test_create_task_metadata_source_with_rendering_options(
    unique_id, resource_type, has_freshness, source_rendering_behavior, expected_id, expected_operator_class, caplog
):
    child_node = DbtNode(
        unique_id=unique_id,
        resource_type=resource_type,
        depends_on=[],
        file_path=Path(""),
        tags=[],
        config={},
        has_freshness=has_freshness,
    )

    metadata = create_task_metadata(
        child_node,
        execution_mode=ExecutionMode.LOCAL,
        source_rendering_behavior=source_rendering_behavior,
        args={},
        dbt_dag_task_group_identifier="",
    )
    if metadata:
        assert metadata.id == expected_id
        assert metadata.operator_class == expected_operator_class


@pytest.mark.parametrize("use_task_group", (None, True, False))
def test_create_task_metadata_seed(caplog, use_task_group):
    sample_node = DbtNode(
        unique_id=f"{DbtResourceType.SEED.value}.my_folder.my_seed",
        resource_type=DbtResourceType.SEED,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    if use_task_group is None:
        metadata = create_task_metadata(
            sample_node, execution_mode=ExecutionMode.DOCKER, args={}, dbt_dag_task_group_identifier=""
        )
    else:
        metadata = create_task_metadata(
            sample_node,
            execution_mode=ExecutionMode.DOCKER,
            args={},
            dbt_dag_task_group_identifier="",
            use_task_group=use_task_group,
        )

    if not use_task_group:
        assert metadata.id == "my_seed_seed"
    else:
        assert metadata.id == "seed"

    assert metadata.operator_class == "cosmos.operators.docker.DbtSeedDockerOperator"
    assert metadata.arguments == {"models": "my_seed"}


def test_create_task_metadata_snapshot(caplog):
    sample_node = DbtNode(
        unique_id=f"{DbtResourceType.SNAPSHOT.value}.my_folder.my_snapshot",
        resource_type=DbtResourceType.SNAPSHOT,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    metadata = create_task_metadata(
        sample_node, execution_mode=ExecutionMode.KUBERNETES, args={}, dbt_dag_task_group_identifier=""
    )
    assert metadata.id == "my_snapshot_snapshot"
    assert metadata.operator_class == "cosmos.operators.kubernetes.DbtSnapshotKubernetesOperator"
    assert metadata.arguments == {"models": "my_snapshot"}


def _normalize_task_id(node: DbtNode) -> str:
    """for test_create_task_metadata_normalize_task_id"""
    return f"new_task_id_{node.name}_{node.resource_type.value}"


@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.9"),
    reason="Airflow task did not have display_name until the 2.9 release",
)
@pytest.mark.parametrize(
    "node_type,node_id,normalize_task_id,use_task_group,expected_node_id,expected_display_name",
    [
        # normalize_task_id is None (default)
        (
            DbtResourceType.MODEL,
            f"{DbtResourceType.MODEL.value}.my_folder.test_node",
            None,
            False,
            "test_node_run",
            None,
        ),
        (
            DbtResourceType.SOURCE,
            f"{DbtResourceType.SOURCE.value}.my_folder.test_node",
            None,
            False,
            "test_node_source",
            None,
        ),
        (
            DbtResourceType.SEED,
            f"{DbtResourceType.SEED.value}.my_folder.test_node",
            None,
            False,
            "test_node_seed",
            None,
        ),
        # normalize_task_id is passed and use_task_group is False
        (
            DbtResourceType.MODEL,
            f"{DbtResourceType.MODEL.value}.my_folder.test_node",
            _normalize_task_id,
            False,
            "new_task_id_test_node_model",
            "test_node_run",
        ),
        (
            DbtResourceType.SOURCE,
            f"{DbtResourceType.MODEL.value}.my_folder.test_node",
            _normalize_task_id,
            False,
            "new_task_id_test_node_source",
            "test_node_source",
        ),
        (
            DbtResourceType.SEED,
            f"{DbtResourceType.MODEL.value}.my_folder.test_node",
            _normalize_task_id,
            False,
            "new_task_id_test_node_seed",
            "test_node_seed",
        ),
        # normalize_task_id is passed and use_task_group is True
        (
            DbtResourceType.MODEL,
            f"{DbtResourceType.MODEL.value}.my_folder.test_node",
            _normalize_task_id,
            True,
            "run",
            None,
        ),
        (
            DbtResourceType.SOURCE,
            f"{DbtResourceType.MODEL.value}.my_folder.test_node",
            _normalize_task_id,
            True,
            "source",
            None,
        ),
        (
            DbtResourceType.SEED,
            f"{DbtResourceType.MODEL.value}.my_folder.test_node",
            _normalize_task_id,
            True,
            "seed",
            None,
        ),
    ],
)
def test_create_task_metadata_normalize_task_id(
    node_type, node_id, normalize_task_id, use_task_group, expected_node_id, expected_display_name
):
    node = DbtNode(
        unique_id=node_id,
        resource_type=node_type,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    args = {}
    metadata = create_task_metadata(
        node,
        execution_mode=ExecutionMode.LOCAL,
        args=args,
        dbt_dag_task_group_identifier="",
        use_task_group=use_task_group,
        normalize_task_id=normalize_task_id,
        source_rendering_behavior=SourceRenderingBehavior.ALL,
    )
    assert metadata.id == expected_node_id
    if expected_display_name:
        assert metadata.arguments["task_display_name"] == expected_display_name
    else:
        assert "task_display_name" not in metadata.arguments


@pytest.mark.parametrize(
    "node_type,node_unique_id,test_indirect_selection,additional_arguments",
    [
        (
            DbtResourceType.MODEL,
            f"{DbtResourceType.MODEL.value}.my_folder.node_name",
            TestIndirectSelection.EAGER,
            {"models": "node_name"},
        ),
        (
            DbtResourceType.MODEL,
            f"{DbtResourceType.MODEL.value}.my_folder.node_name.v1",
            TestIndirectSelection.EAGER,
            {"models": "node_name.v1"},
        ),
        (
            DbtResourceType.SEED,
            f"{DbtResourceType.SEED.value}.my_folder.node_name",
            TestIndirectSelection.CAUTIOUS,
            {"select": "node_name", "indirect_selection": "cautious"},
        ),
        (
            DbtResourceType.SOURCE,
            f"{DbtResourceType.SOURCE.value}.my_folder.node_name",
            TestIndirectSelection.BUILDABLE,
            {"select": "source:node_name", "indirect_selection": "buildable"},
        ),
        (
            DbtResourceType.SNAPSHOT,
            f"{DbtResourceType.SNAPSHOT.value}.my_folder.node_name",
            TestIndirectSelection.EMPTY,
            {"select": "node_name", "indirect_selection": "empty"},
        ),
    ],
)
def test_create_test_task_metadata(node_type, node_unique_id, test_indirect_selection, additional_arguments):
    sample_node = DbtNode(
        unique_id=node_unique_id,
        resource_type=node_type,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    metadata = create_test_task_metadata(
        test_task_name="test_no_nulls",
        execution_mode=ExecutionMode.LOCAL,
        test_indirect_selection=test_indirect_selection,
        task_args={"task_arg": "value"},
        on_warning_callback=True,
        node=sample_node,
    )
    assert metadata.id == "test_no_nulls"
    assert metadata.operator_class == "cosmos.operators.local.DbtTestLocalOperator"
    assert metadata.arguments == {
        **{
            "task_arg": "value",
            "on_warning_callback": True,
        },
        **additional_arguments,
    }


@pytest.mark.parametrize(
    "input,expected", [("snake_case", "SnakeCase"), ("snake_case_with_underscores", "SnakeCaseWithUnderscores")]
)
def test_snake_case_to_camelcase(input, expected):
    assert _snake_case_to_camelcase(input) == expected


def test_airflow_kwargs_generation():
    """
    airflow_kwargs_generation should always contain dag.
    """
    task_args = {
        "group_id": "fake_group_id",
        "project_dir": SAMPLE_PROJ_PATH,
        "conn_id": "fake_conn",
        "render_config": RenderConfig(select=["fake-render"], source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR),
        "default_args": {"retries": 2},
        "profile_config": ProfileConfig(
            profile_name="default",
            target_name="default",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="fake_conn",
                profile_args={"schema": "public"},
            ),
        ),
        "dag": DAG(dag_id="fake_dag_name"),
    }
    result = airflow_kwargs(**task_args)

    assert "dag" in result


@pytest.mark.parametrize(
    "dbt_extra_config,expected_owner",
    [
        ({}, DEFAULT_OWNER),
        ({"meta": {}}, DEFAULT_OWNER),
        ({"meta": {"owner": ""}}, DEFAULT_OWNER),
        ({"meta": {"owner": "dbt-owner"}}, "dbt-owner"),
    ],
)
def test_owner(dbt_extra_config, expected_owner):
    with DAG("test-task-group-after-each", start_date=datetime(2022, 1, 1)) as dag:
        node = DbtNode(
            unique_id=f"{DbtResourceType.MODEL.value}.my_folder.my_model",
            resource_type=DbtResourceType.MODEL,
            file_path=SAMPLE_PROJ_PATH / "gen2/models/parent.sql",
            tags=["has_child"],
            config={"materialized": "view", **dbt_extra_config},
            depends_on=[],
        )

    output: TaskGroup = generate_task_or_group(
        dag=dag,
        task_group=None,
        node=node,
        execution_mode=ExecutionMode.LOCAL,
        test_indirect_selection=TestIndirectSelection.EAGER,
        task_args={
            "project_dir": SAMPLE_PROJ_PATH,
            "profile_config": ProfileConfig(
                profile_name="default",
                target_name="default",
                profile_mapping=PostgresUserPasswordProfileMapping(
                    conn_id="fake_conn",
                    profile_args={"schema": "public"},
                ),
            ),
        },
        test_behavior=TestBehavior.AFTER_EACH,
        on_warning_callback=None,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )

    assert len(output.leaves) == 1
    assert output.leaves[0].owner == expected_owner


def test_custom_meta():
    with DAG("test-id", start_date=datetime(2022, 1, 1)) as dag:
        task_args = {
            "project_dir": SAMPLE_PROJ_PATH,
            "conn_id": "fake_conn",
            "profile_config": ProfileConfig(
                profile_name="default",
                target_name="default",
                profile_mapping=PostgresUserPasswordProfileMapping(
                    conn_id="fake_conn",
                    profile_args={"schema": "public"},
                ),
            ),
        }
        build_airflow_graph(
            nodes=sample_nodes,
            dag=dag,
            execution_mode=ExecutionMode.LOCAL,
            test_indirect_selection=TestIndirectSelection.EAGER,
            task_args=task_args,
            render_config=RenderConfig(
                test_behavior=TestBehavior.AFTER_EACH,
                source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
            ),
            dbt_project_name="astro_shop",
        )
        # test custom meta (queue, pool)
        for task in dag.tasks:
            if task.task_id == "child2_v2_run":
                assert task.pool == "custom_pool"
            else:
                assert task.pool == "default_pool"

            if task.task_id == "child_run":
                assert task.queue == "custom_queue"
            else:
                assert task.queue == "default"
