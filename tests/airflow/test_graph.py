from cosmos.airflow.graph import calculate_leaves, create_task_metadata
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


def test_create_task_metadata(caplog):
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
