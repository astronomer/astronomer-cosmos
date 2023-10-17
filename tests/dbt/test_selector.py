from pathlib import Path

import pytest

from cosmos.dbt.selector import SelectorConfig
from cosmos.constants import DbtResourceType
from cosmos.dbt.graph import DbtNode
from cosmos.dbt.selector import select_nodes
from cosmos.exceptions import CosmosValueError

SAMPLE_PROJ_PATH = Path("/home/user/path/dbt-proj/")


@pytest.fixture
def selector_config():
    project_dir = Path("/path/to/project")
    statement = ""
    return SelectorConfig(project_dir, statement)


@pytest.mark.parametrize(
    "paths, tags, config, other, expected",
    [
        ([], [], {}, [], True),
        ([Path("path1")], [], {}, [], False),
        ([], ["tag:has_child"], {}, [], False),
        ([], [], {"config.tags:test"}, [], False),
        ([], [], {}, ["other"], False),
        ([Path("path1")], ["tag:has_child"], {"config.tags:test"}, ["other"], False),
    ],
)
def test_is_empty_config(selector_config, paths, tags, config, other, expected):
    selector_config.paths = paths
    selector_config.tags = tags
    selector_config.config = config
    selector_config.other = other

    assert selector_config.is_empty == expected


grandparent_node = DbtNode(
    name="grandparent",
    unique_id="grandparent",
    resource_type=DbtResourceType.MODEL,
    depends_on=[],
    file_path=SAMPLE_PROJ_PATH / "gen1/models/grandparent.sql",
    tags=["has_child"],
    config={"materialized": "view", "tags": ["has_child"]},
)
parent_node = DbtNode(
    name="parent",
    unique_id="parent",
    resource_type=DbtResourceType.MODEL,
    depends_on=["grandparent"],
    file_path=SAMPLE_PROJ_PATH / "gen2/models/parent.sql",
    tags=["has_child", "is_child"],
    config={"materialized": "view", "tags": ["has_child", "is_child"]},
)
child_node = DbtNode(
    name="child",
    unique_id="child",
    resource_type=DbtResourceType.MODEL,
    depends_on=["parent"],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/child.sql",
    tags=["nightly", "is_child"],
    config={"materialized": "table", "tags": ["nightly", "is_child"]},
)

grandchild_1_test_node = DbtNode(
    name="grandchild_1",
    unique_id="grandchild_1",
    resource_type=DbtResourceType.MODEL,
    depends_on=["parent"],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/grandchild_1.sql",
    tags=["nightly", "deprecated", "test"],
    config={"materialized": "table", "tags": ["nightly", "deprecated", "test"]},
)

grandchild_2_test_node = DbtNode(
    name="grandchild_2",
    unique_id="grandchild_2",
    resource_type=DbtResourceType.MODEL,
    depends_on=["parent"],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/grandchild_2.sql",
    tags=["nightly", "deprecated", "test2"],
    config={"materialized": "table", "tags": ["nightly", "deprecated", "test2"]},
)

sample_nodes = {
    grandparent_node.unique_id: grandparent_node,
    parent_node.unique_id: parent_node,
    child_node.unique_id: child_node,
    grandchild_1_test_node.unique_id: grandchild_1_test_node,
    grandchild_2_test_node.unique_id: grandchild_2_test_node,
}


def test_select_nodes_by_select_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["tag:has_child"])
    expected = {grandparent_node.unique_id: grandparent_node, parent_node.unique_id: parent_node}
    assert selected == expected


def test_select_nodes_by_select_config():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.materialized:table"])
    expected = {
        child_node.unique_id: child_node,
        grandchild_1_test_node.unique_id: grandchild_1_test_node,
        grandchild_2_test_node.unique_id: grandchild_2_test_node,
    }
    assert selected == expected


def test_select_nodes_by_select_config_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.tags:is_child"])
    expected = {
        child_node.unique_id: child_node,
    }
    assert selected == expected


def test_select_nodes_by_select_union_config_tag():
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.tags:is_child", "config.materialized:view"]
    )
    expected = {
        grandparent_node.unique_id: grandparent_node,
        parent_node.unique_id: parent_node,
        child_node.unique_id: child_node,
    }
    assert selected == expected


def test_select_nodes_by_select_union_config_test_tags():
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=sample_nodes,
        select=["config.tags:test", "config.tags:test2", "config.materialized:view"],
    )
    expected = {
        grandparent_node.unique_id: grandparent_node,
        parent_node.unique_id: parent_node,
        grandchild_1_test_node.unique_id: grandchild_1_test_node,
        grandchild_2_test_node.unique_id: grandchild_2_test_node,
    }
    assert selected == expected


def test_select_nodes_by_select_intersection_config_tag():
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.tags:is_child,config.materialized:view"]
    )
    assert selected == {}


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
        grandchild_1_test_node.unique_id: grandchild_1_test_node,
        grandchild_2_test_node.unique_id: grandchild_2_test_node,
    }
    assert selected == expected


def test_select_nodes_by_select_intersection():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["tag:has_child,tag:nightly"])
    assert selected == {}


def test_select_nodes_by_exclude_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["tag:has_child"])
    expected = {
        child_node.unique_id: child_node,
        grandchild_1_test_node.unique_id: grandchild_1_test_node,
        grandchild_2_test_node.unique_id: grandchild_2_test_node,
    }
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


def test_select_nodes_by_exclude_union_config_test_tags():
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["config.tags:test", "config.tags:test2"]
    )
    expected = {
        grandparent_node.unique_id: grandparent_node,
        parent_node.unique_id: parent_node,
        child_node.unique_id: child_node,
    }
    assert selected == expected


def test_select_nodes_by_path_dir():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["path:gen3/models"])
    expected = {
        child_node.unique_id: child_node,
        grandchild_1_test_node.unique_id: grandchild_1_test_node,
        grandchild_2_test_node.unique_id: grandchild_2_test_node,
    }
    assert selected == expected


def test_select_nodes_by_path_file():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["path:gen2/models/parent.sql"])
    expected = {
        parent_node.unique_id: parent_node,
    }
    assert selected == expected
