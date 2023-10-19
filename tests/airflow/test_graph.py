from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow import __version__ as airflow_version
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from packaging import version

from cosmos.airflow.graph import (
    build_airflow_graph,
    calculate_leaves,
    calculate_operator_class,
    create_task_metadata,
    create_test_task_metadata,
    generate_task_or_group,
)
from cosmos.config import ProfileConfig
from cosmos.constants import DbtResourceType, ExecutionMode, TestBehavior
from cosmos.dbt.graph import DbtNode
from cosmos.profiles import PostgresUserPasswordProfileMapping

SAMPLE_PROJ_PATH = Path("/home/user/path/dbt-proj/")

parent_seed = DbtNode(
    name="seed_parent",
    unique_id="seed_parent",
    resource_type=DbtResourceType.SEED,
    depends_on=[],
    file_path="",
)
parent_node = DbtNode(
    name="parent",
    unique_id="parent",
    resource_type=DbtResourceType.MODEL,
    depends_on=["seed_parent"],
    file_path=SAMPLE_PROJ_PATH / "gen2/models/parent.sql",
    tags=["has_child"],
    config={"materialized": "view"},
    has_test=True,
)
test_parent_node = DbtNode(
    name="test_parent", unique_id="test_parent", resource_type=DbtResourceType.TEST, depends_on=["parent"], file_path=""
)
child_node = DbtNode(
    name="child",
    unique_id="child",
    resource_type=DbtResourceType.MODEL,
    depends_on=["parent"],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/child.sql",
    tags=["nightly"],
    config={"materialized": "table"},
)

sample_nodes_list = [parent_seed, parent_node, test_parent_node, child_node]
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
            task_args=task_args,
            test_behavior=TestBehavior.AFTER_EACH,
            dbt_project_name="astro_shop",
        )
    topological_sort = [task.task_id for task in dag.topological_sort()]
    expected_sort = [
        "seed_parent_seed",
        "parent.run",
        "parent.test",
        "child_run",
    ]

    assert topological_sort == expected_sort
    task_groups = dag.task_group_dict
    assert len(task_groups) == 1

    assert task_groups["parent"].upstream_task_ids == {"seed_parent_seed"}
    assert list(task_groups["parent"].children.keys()) == ["parent.run", "parent.test"]

    assert len(dag.leaves) == 1
    assert dag.leaves[0].task_id == "child_run"


@pytest.mark.parametrize(
    "node_type,task_suffix",
    [(DbtResourceType.MODEL, "run"), (DbtResourceType.SEED, "seed"), (DbtResourceType.SNAPSHOT, "snapshot")],
)
def test_create_task_group_for_after_each_supported_nodes(node_type, task_suffix):
    """
    dbt test runs tests defined on models, sources, snapshots, and seeds.
    It expects that you have already created those resources through the appropriate commands.
    https://docs.getdbt.com/reference/commands/test
    """
    with DAG("test-task-group-after-each", start_date=datetime(2022, 1, 1)) as dag:
        node = DbtNode(
            name="dbt_node",
            unique_id="dbt_node",
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
        build_airflow_graph(
            nodes=sample_nodes,
            dag=dag,
            execution_mode=ExecutionMode.LOCAL,
            task_args=task_args,
            test_behavior=TestBehavior.AFTER_ALL,
            dbt_project_name="astro_shop",
        )
    topological_sort = [task.task_id for task in dag.topological_sort()]
    expected_sort = ["seed_parent_seed", "parent_run", "child_run", "astro_shop_test"]
    assert topological_sort == expected_sort

    task_groups = dag.task_group_dict
    assert len(task_groups) == 0

    assert len(dag.leaves) == 1
    assert dag.leaves[0].task_id == "astro_shop_test"


def test_calculate_operator_class():
    class_module_import_path = calculate_operator_class(execution_mode=ExecutionMode.KUBERNETES, dbt_class="DbtSeed")
    assert class_module_import_path == "cosmos.operators.kubernetes.DbtSeedKubernetesOperator"


def test_calculate_leaves():
    grandparent_node = DbtNode(
        name="grandparent",
        unique_id="grandparent",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    parent1_node = DbtNode(
        name="parent1",
        unique_id="parent1",
        resource_type=DbtResourceType.MODEL,
        depends_on=["grandparent"],
        file_path="",
        tags=[],
        config={},
    )
    parent2_node = DbtNode(
        name="parent2",
        unique_id="parent2",
        resource_type=DbtResourceType.MODEL,
        depends_on=["grandparent"],
        file_path="",
        tags=[],
        config={},
    )
    child_node = DbtNode(
        name="child",
        unique_id="child",
        resource_type=DbtResourceType.MODEL,
        depends_on=["parent1", "parent2"],
        file_path="",
        tags=[],
        config={},
    )

    nodes_list = [grandparent_node, parent1_node, parent2_node, child_node]
    nodes = {node.unique_id: node for node in nodes_list}

    leaves = calculate_leaves(nodes.keys(), nodes)
    assert leaves == ["child"]


@patch("cosmos.airflow.graph.logger.propagate", True)
def test_create_task_metadata_unsupported(caplog):
    child_node = DbtNode(
        name="unsupported",
        unique_id="unsupported",
        resource_type="unsupported",
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    response = create_task_metadata(child_node, execution_mode="", args={})
    assert response is None
    expected_msg = (
        "Unavailable conversion function for <unsupported> (node <unsupported>). "
        "Define a converter function using render_config.node_converters."
    )
    assert caplog.messages[0] == expected_msg


def test_create_task_metadata_model(caplog):
    child_node = DbtNode(
        name="my_model",
        unique_id="my_folder.my_model",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    metadata = create_task_metadata(child_node, execution_mode=ExecutionMode.LOCAL, args={})
    assert metadata.id == "my_model_run"
    assert metadata.operator_class == "cosmos.operators.local.DbtRunLocalOperator"
    assert metadata.arguments == {"models": "my_model"}


def test_create_task_metadata_model_use_task_group(caplog):
    child_node = DbtNode(
        name="my_model",
        unique_id="my_folder.my_model",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path=Path(""),
        tags=[],
        config={},
    )
    metadata = create_task_metadata(child_node, execution_mode=ExecutionMode.LOCAL, args={}, use_task_group=True)
    assert metadata.id == "run"


@pytest.mark.parametrize("use_task_group", (None, True, False))
def test_create_task_metadata_seed(caplog, use_task_group):
    sample_node = DbtNode(
        name="my_seed",
        unique_id="my_folder.my_seed",
        resource_type=DbtResourceType.SEED,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    if use_task_group is None:
        metadata = create_task_metadata(sample_node, execution_mode=ExecutionMode.DOCKER, args={})
    else:
        metadata = create_task_metadata(
            sample_node,
            execution_mode=ExecutionMode.DOCKER,
            args={},
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
        name="my_snapshot",
        unique_id="my_folder.my_snapshot",
        resource_type=DbtResourceType.SNAPSHOT,
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    metadata = create_task_metadata(sample_node, execution_mode=ExecutionMode.KUBERNETES, args={})
    assert metadata.id == "my_snapshot_snapshot"
    assert metadata.operator_class == "cosmos.operators.kubernetes.DbtSnapshotKubernetesOperator"
    assert metadata.arguments == {"models": "my_snapshot"}


@pytest.mark.parametrize(
    "node_type,node_unique_id,selector_key,selector_value",
    [
        (DbtResourceType.MODEL, "node_name", "models", "node_name"),
        (DbtResourceType.SEED, "node_name", "select", "node_name"),
        (DbtResourceType.SOURCE, "source.node_name", "select", "source:node_name"),
        (DbtResourceType.SNAPSHOT, "node_name", "select", "node_name"),
    ],
)
def test_create_test_task_metadata(node_type, node_unique_id, selector_key, selector_value):
    sample_node = DbtNode(
        name="node_name",
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
        task_args={"task_arg": "value"},
        on_warning_callback=True,
        node=sample_node,
    )
    assert metadata.id == "test_no_nulls"
    assert metadata.operator_class == "cosmos.operators.local.DbtTestLocalOperator"
    assert metadata.arguments == {"task_arg": "value", "on_warning_callback": True, selector_key: selector_value}
