from cosmos.airflow.graph import calculate_leaves, create_task_metadata, create_test_task_metadata
from cosmos.dbt.graph import DbtNode


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

    nodes = [grandparent_node, parent1_node, parent2_node, child_node]

    leaves = calculate_leaves(nodes)
    assert leaves == [child_node]


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
    response = create_task_metadata(child_node, execution_mode="", args=[])
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
    metadata = create_task_metadata(child_node, execution_mode="local", args=[])
    assert metadata.id == "my_model_run"
    assert metadata.operator_class == "cosmos.operators.local.DbtRunLocalOperator"


def test_create_task_metadata_seed(caplog):
    child_node = DbtNode(
        name="my_seed",
        unique_id="my_folder.my_seed",
        resource_type="seed",
        depends_on=[],
        file_path="",
        tags=[],
        config={},
    )
    metadata = create_task_metadata(child_node, execution_mode="docker", args=[])
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
