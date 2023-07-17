from pathlib import Path
from datetime import datetime

import pytest
from airflow import __version__ as airflow_version
from airflow.models import DAG
from packaging import version

from cosmos.airflow.graph import (
    build_airflow_graph,
    calculate_leaves,
    create_task_metadata,
    create_test_task_metadata,
    calculate_operator_class,
)
from cosmos.dbt.graph import DbtNode


SAMPLE_PROJ_PATH = Path("/home/user/path/dbt-proj/")

parent_seed = DbtNode(
    name="seed_parent",
    unique_id="seed_parent",
    resource_type="seed",
    depends_on=[],
    file_path="",
)
parent_node = DbtNode(
    name="parent",
    unique_id="parent",
    resource_type="model",
    depends_on=["seed_parent"],
    file_path=SAMPLE_PROJ_PATH / "gen2/models/parent.sql",
    tags=["has_child"],
    config={"materialized": "view"},
)
test_parent_node = DbtNode(
    name="test_parent", unique_id="test_parent", resource_type="test", depends_on=["parent"], file_path=""
)
child_node = DbtNode(
    name="child",
    unique_id="child",
    resource_type="model",
    depends_on=["parent"],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/child.sql",
    tags=["nightly"],
    config={"materialized": "table"},
)
test_child_node = DbtNode(
    name="test_child",
    unique_id="test_child",
    resource_type="test",
    depends_on=["child"],
    file_path="",
)

sample_nodes_list = [parent_seed, parent_node, test_parent_node, child_node, test_child_node]
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
        }
        build_airflow_graph(
            nodes=sample_nodes,
            dag=dag,
            execution_mode="local",
            task_args=task_args,
            test_behavior="after_each",
            dbt_project_name="astro_shop",
            conn_id="fake_conn",
        )
    topological_sort = [task.task_id for task in dag.topological_sort()]
    expected_sort = [
        "seed_parent_seed",
        "parent.parent_run",
        "parent.parent_test",
        "child.child_run",
        "child.child_test",
    ]
    assert topological_sort == expected_sort
    task_groups = dag.task_group_dict
    assert len(task_groups) == 2

    assert task_groups["parent"].upstream_task_ids == {"seed_parent_seed"}
    assert list(task_groups["parent"].children.keys()) == ["parent.parent_run", "parent.parent_test"]

    assert task_groups["child"].upstream_task_ids == {"parent.parent_test"}
    assert list(task_groups["child"].children.keys()) == ["child.child_run", "child.child_test"]

    assert len(dag.leaves) == 1
    assert dag.leaves[0].task_id == "child.child_test"


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
        }
        build_airflow_graph(
            nodes=sample_nodes,
            dag=dag,
            execution_mode="local",
            task_args=task_args,
            test_behavior="after_all",
            dbt_project_name="astro_shop",
            conn_id="fake_conn",
        )
    topological_sort = [task.task_id for task in dag.topological_sort()]
    expected_sort = ["seed_parent_seed", "parent_run", "child_run", "astro_shop_test"]
    assert topological_sort == expected_sort

    task_groups = dag.task_group_dict
    assert len(task_groups) == 0

    assert len(dag.leaves) == 1
    assert dag.leaves[0].task_id == "astro_shop_test"


def test_calculate_operator_class():
    class_module_import_path = calculate_operator_class(execution_mode="kubernetes", dbt_class="Seed")
    assert class_module_import_path == "cosmos.operators.kubernetes.SeedKubernetesOperator"


def test_calculate_leaves():
    grandparent_node = DbtNode(
        name="grandparent",
        unique_id="grandparent",
        resource_type="model",
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    parent1_node = DbtNode(
        name="parent1",
        unique_id="parent1",
        resource_type="model",
        depends_on=["grandparent"],
        file_path="",
        tags=[],
        config={},
    )
    parent2_node = DbtNode(
        name="parent2",
        unique_id="parent2",
        resource_type="model",
        depends_on=["grandparent"],
        file_path="",
        tags=[],
        config={},
    )
    child_node = DbtNode(
        name="child",
        unique_id="child",
        resource_type="model",
        depends_on=["parent1", "parent2"],
        file_path="",
        tags=[],
        config={},
    )

    nodes_list = [grandparent_node, parent1_node, parent2_node, child_node]
    nodes = {node.unique_id: node for node in nodes_list}

    leaves = calculate_leaves(nodes.keys(), nodes)
    assert leaves == ["child"]


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
    expected_msg = "Unsupported resource type unsupported (node unsupported)."
    assert caplog.messages[0] == expected_msg


def test_create_task_metadata_model(caplog):
    child_node = DbtNode(
        name="my_model",
        unique_id="my_folder.my_model",
        resource_type="model",
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    metadata = create_task_metadata(child_node, execution_mode="local", args={})
    assert metadata.id == "my_model_run"
    assert metadata.operator_class == "cosmos.operators.local.DbtRunLocalOperator"
    assert metadata.arguments == {"models": "my_model"}


def test_create_task_metadata_seed(caplog):
    sample_node = DbtNode(
        name="my_seed",
        unique_id="my_folder.my_seed",
        resource_type="seed",
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    metadata = create_task_metadata(sample_node, execution_mode="docker", args={})
    assert metadata.id == "my_seed_seed"
    assert metadata.operator_class == "cosmos.operators.docker.DbtSeedDockerOperator"


def test_create_test_task_metadata():
    metadata = create_test_task_metadata(
        test_task_name="test_no_nulls",
        execution_mode="local",
        task_args={"task_arg": "value"},
        on_warning_callback=True,
        model_name="my_model",
    )
    assert metadata.id == "test_no_nulls"
    assert metadata.operator_class == "cosmos.operators.local.DbtTestLocalOperator"
    assert metadata.arguments == {"task_arg": "value", "on_warning_callback": True, "models": "my_model"}
