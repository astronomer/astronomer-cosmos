from pathlib import Path

import pytest

from cosmos.constants import DbtResourceType
from cosmos.dbt.graph import DbtNode
from cosmos.dbt.selector import select_nodes
from cosmos.exceptions import CosmosValueError

SAMPLE_PROJ_PATH = Path("/home/user/path/dbt-proj/")

grandparent_node = DbtNode(
    name="grandparent",
    unique_id="grandparent",
    resource_type=DbtResourceType.MODEL,
    depends_on=[],
    file_path=SAMPLE_PROJ_PATH / "gen1/models/grandparent.sql",
    tags=["has_child"],
    config={"materialized": "view"},
)
parent_node = DbtNode(
    name="parent",
    unique_id="parent",
    resource_type=DbtResourceType.MODEL,
    depends_on=["grandparent"],
    file_path=SAMPLE_PROJ_PATH / "gen2/models/parent.sql",
    tags=["has_child"],
    config={"materialized": "view"},
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

sample_nodes = {
    grandparent_node.unique_id: grandparent_node,
    parent_node.unique_id: parent_node,
    child_node.unique_id: child_node,
}


def test_select_nodes_by_select_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["tag:has_child"])
    expected = {grandparent_node.unique_id: grandparent_node, parent_node.unique_id: parent_node}
    assert selected == expected


def test_select_nodes_by_select_config():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.materialized:table"])
    expected = {child_node.unique_id: child_node}
    assert selected == expected


def test_select_nodes_by_select_path():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["path:gen2/models"])
    expected = {
        parent_node.unique_id: parent_node,
    }
    assert selected == expected


def test_select_nodes_by_select_union():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["tag:has_child", "tag:nightly"])
    expected = {
        grandparent_node.unique_id: grandparent_node,
        parent_node.unique_id: parent_node,
        child_node.unique_id: child_node,
    }
    assert selected == expected


def test_select_nodes_by_select_intersection():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["tag:has_child,tag:nightly"])
    assert selected == {}


def test_select_nodes_by_exclude_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["tag:has_child"])
    expected = {child_node.unique_id: child_node}
    assert selected == expected


def test_select_nodes_by_exclude_unsupported_selector():
    with pytest.raises(CosmosValueError) as err_info:
        assert select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["unsupported:filter"])
    assert err_info.value.args[0] == "Invalid exclude filter: unsupported:filter"


def test_select_nodes_by_select_union_exclude_tags():
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.materialized:view"], exclude=["tag:has_child"]
    )
    expected = {}
    assert selected == expected
