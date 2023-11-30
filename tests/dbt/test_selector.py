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
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.grandparent",
    resource_type=DbtResourceType.MODEL,
    depends_on=[],
    file_path=SAMPLE_PROJ_PATH / "gen1/models/grandparent.sql",
    tags=["has_child"],
    config={"materialized": "view", "tags": ["has_child"]},
)

another_grandparent_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.another_grandparent_node",
    resource_type=DbtResourceType.MODEL,
    depends_on=[],
    file_path=SAMPLE_PROJ_PATH / "gen1/models/another_grandparent_node.sql",
    tags=[],
    config={},
)

parent_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.parent",
    resource_type=DbtResourceType.MODEL,
    depends_on=[grandparent_node.unique_id, another_grandparent_node.unique_id],
    file_path=SAMPLE_PROJ_PATH / "gen2/models/parent.sql",
    tags=["has_child", "is_child"],
    config={"materialized": "view", "tags": ["has_child", "is_child"]},
)

child_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.child",
    resource_type=DbtResourceType.MODEL,
    depends_on=[parent_node.unique_id],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/child.sql",
    tags=["nightly", "is_child"],
    config={"materialized": "table", "tags": ["nightly", "is_child"]},
)

sibling1_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.sibling1",
    resource_type=DbtResourceType.MODEL,
    depends_on=[parent_node.unique_id],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/sibling1.sql",
    tags=["nightly", "deprecated", "test"],
    config={"materialized": "table", "tags": ["nightly", "deprecated", "test"]},
)

sibling2_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.sibling2",
    resource_type=DbtResourceType.MODEL,
    depends_on=[parent_node.unique_id],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/sibling2.sql",
    tags=["nightly", "deprecated", "test2"],
    config={"materialized": "table", "tags": ["nightly", "deprecated", "test2"]},
)

orphaned_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.orphaned",
    resource_type=DbtResourceType.MODEL,
    depends_on=[],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/orphaned.sql",
    tags=[],
    config={},
)

sample_nodes = {
    grandparent_node.unique_id: grandparent_node,
    another_grandparent_node.unique_id: another_grandparent_node,
    parent_node.unique_id: parent_node,
    child_node.unique_id: child_node,
    sibling1_node.unique_id: sibling1_node,
    sibling2_node.unique_id: sibling2_node,
    orphaned_node.unique_id: orphaned_node,
}


def test_select_nodes_by_select_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["tag:has_child"])
    expected = {grandparent_node.unique_id: grandparent_node, parent_node.unique_id: parent_node}
    assert selected == expected


def test_select_nodes_by_select_config():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.materialized:table"])
    expected = {
        child_node.unique_id: child_node,
        sibling1_node.unique_id: sibling1_node,
        sibling2_node.unique_id: sibling2_node,
    }
    assert selected == expected


def test_select_nodes_by_select_config_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.tags:is_child"])
    expected = {
        parent_node.unique_id: parent_node,
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
        sibling1_node.unique_id: sibling1_node,
        sibling2_node.unique_id: sibling2_node,
    }
    assert selected == expected


def test_select_nodes_by_select_intersection_tag():
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["tag:is_child,config.materialized:view"]
    )
    expected = {
        parent_node.unique_id: parent_node,
    }
    assert selected == expected


def test_select_nodes_by_select_intersection_config_tag():
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.tags:is_child,config.materialized:view"]
    )
    expected = {
        parent_node.unique_id: parent_node,
    }
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
        sibling1_node.unique_id: sibling1_node,
        sibling2_node.unique_id: sibling2_node,
    }
    assert selected == expected


def test_select_nodes_by_select_intersection():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["tag:has_child,tag:nightly"])
    assert selected == {}


def test_select_nodes_by_exclude_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["tag:has_child"])
    expected = {
        child_node.unique_id: child_node,
        sibling1_node.unique_id: sibling1_node,
        sibling2_node.unique_id: sibling2_node,
        another_grandparent_node.unique_id: another_grandparent_node,
        orphaned_node.unique_id: orphaned_node,
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
        another_grandparent_node.unique_id: another_grandparent_node,
        parent_node.unique_id: parent_node,
        child_node.unique_id: child_node,
        orphaned_node.unique_id: orphaned_node,
    }
    assert selected == expected


def test_select_nodes_by_path_dir():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["path:gen3/models"])
    expected = {
        child_node.unique_id: child_node,
        sibling1_node.unique_id: sibling1_node,
        sibling2_node.unique_id: sibling2_node,
        orphaned_node.unique_id: orphaned_node,
    }
    assert selected == expected


def test_select_nodes_by_path_file():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["path:gen2/models/parent.sql"])
    expected = [parent_node.unique_id]
    assert list(selected.keys()) == expected


def test_select_nodes_by_child_and_precursors():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["+child"])
    expected = [
        another_grandparent_node.unique_id,
        child_node.unique_id,
        grandparent_node.unique_id,
        parent_node.unique_id,
    ]
    assert sorted(selected.keys()) == expected


def test_select_nodes_by_child_and_precursors_exclude_tags():
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["+child"], exclude=["tag:has_child"]
    )
    expected = [another_grandparent_node.unique_id, child_node.unique_id]
    assert sorted(selected.keys()) == expected


def test_select_node_by_child_and_precursors_partial_tree():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["+parent"])
    expected = [another_grandparent_node.unique_id, grandparent_node.unique_id, parent_node.unique_id]
    assert sorted(selected.keys()) == expected


def test_select_node_by_precursors_with_orphaned_node():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["+orphaned"])
    expected = [orphaned_node.unique_id]
    assert list(selected.keys()) == expected


def test_select_nodes_by_child_and_first_degree_precursors():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["1+child"])
    expected = [
        child_node.unique_id,
        parent_node.unique_id,
    ]
    assert sorted(selected.keys()) == expected


def test_select_nodes_by_child_and_second_degree_precursors():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["2+child"])
    expected = [
        another_grandparent_node.unique_id,
        child_node.unique_id,
        grandparent_node.unique_id,
        parent_node.unique_id,
    ]
    assert sorted(selected.keys()) == expected


def test_select_node_by_exact_node_name():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["child"])
    expected = [child_node.unique_id]
    assert list(selected.keys()) == expected


def test_select_node_by_child_and_precursors_no_node():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["+modelDoesntExist"])
    expected = []
    assert list(selected.keys()) == expected


def test_select_node_by_descendants():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["grandparent+"])
    expected = [
        "model.dbt-proj.child",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
        "model.dbt-proj.sibling1",
        "model.dbt-proj.sibling2",
    ]
    assert sorted(selected.keys()) == expected


def test_select_node_by_descendants_depth_first_degree():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["grandparent+1"])
    expected = [
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
    ]
    assert sorted(selected.keys()) == expected


def test_select_node_by_descendants_union():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["grandparent+1", "parent+1"])
    expected = [
        "model.dbt-proj.child",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
        "model.dbt-proj.sibling1",
        "model.dbt-proj.sibling2",
    ]
    assert sorted(selected.keys()) == expected


def test_select_node_by_descendants_intersection():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["grandparent+1,parent+1"])
    expected = [
        "model.dbt-proj.parent",
    ]
    assert sorted(selected.keys()) == expected


def test_select_node_by_descendants_intersection_with_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["parent+1,tag:has_child"])
    expected = [
        "model.dbt-proj.parent",
    ]
    assert sorted(selected.keys()) == expected


def test_select_node_by_descendants_and_tag_union():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["child", "tag:has_child"])
    expected = [
        "model.dbt-proj.child",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
    ]
    assert sorted(selected.keys()) == expected


def test_exclude_by_graph_selector():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["+parent"])
    expected = [
        "model.dbt-proj.child",
        "model.dbt-proj.orphaned",
        "model.dbt-proj.sibling1",
        "model.dbt-proj.sibling2",
    ]
    assert sorted(selected.keys()) == expected


def test_exclude_by_union_graph_selector_and_tag():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["+parent", "tag:deprecated"])
    expected = [
        "model.dbt-proj.child",
        "model.dbt-proj.orphaned",
    ]
    assert sorted(selected.keys()) == expected
