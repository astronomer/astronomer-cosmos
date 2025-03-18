from pathlib import Path

import pytest

from cosmos.constants import DbtResourceType
from cosmos.dbt.graph import DbtNode
from cosmos.dbt.selector import NodeSelector, SelectorConfig, select_nodes
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
    config={"meta": {"frequency": "daily"}},
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

sibling3_node = DbtNode(
    unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.public.sibling3",
    resource_type=DbtResourceType.MODEL,
    depends_on=[parent_node.unique_id],
    file_path=SAMPLE_PROJ_PATH / "gen3/models/public.sibling3.sql",
    tags=["nightly", "deprecated", "test3"],
    config={"materialized": "table", "tags": ["nightly", "deprecated", "test3"]},
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
    sibling3_node.unique_id: sibling3_node,
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
        sibling3_node.unique_id: sibling3_node,
    }
    assert selected == expected


def test_select_nodes_by_select_config_meta_nested_property():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.meta.frequency:daily"])
    expected = {another_grandparent_node.unique_id: another_grandparent_node}
    assert selected == expected


def test_select_nodes_by_select_config_meta_nested_property_with_children():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["config.meta.frequency:daily+"])
    expected = {
        another_grandparent_node.unique_id: another_grandparent_node,
        parent_node.unique_id: parent_node,
        child_node.unique_id: child_node,
        sibling1_node.unique_id: sibling1_node,
        sibling2_node.unique_id: sibling2_node,
        sibling3_node.unique_id: sibling3_node,
    }
    assert selected == expected


def test_select_nodes_by_select_config_meta_nested_property_two_meta_values():
    local_nodes = dict(sample_nodes)
    someone_else_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.someone_else",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "gen1/models/someone_else.sql",
        tags=[],
        config={"meta": {"frequency": "daily", "dbt_environment": "dev"}},
    )
    local_nodes[someone_else_node.unique_id] = someone_else_node
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        select=["config.meta.frequency:daily,config.meta.dbt_environment:dev"],
    )
    expected = {someone_else_node.unique_id: someone_else_node}
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


def test_select_nodes_by_select_intersection_config_graph_selector_includes_ancestors():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["+child,+sibling1"])
    expected = {
        grandparent_node.unique_id: grandparent_node,
        another_grandparent_node.unique_id: another_grandparent_node,
        parent_node.unique_id: parent_node,
    }
    assert selected == expected


def test_select_nodes_by_select_intersection_config_graph_selector_none():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["+child,+orphaned"])
    expected = {}
    assert selected == expected


def test_select_nodes_by_intersection_and_tag_ancestry():
    parent_sibling_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.parent_sibling",
        resource_type=DbtResourceType.MODEL,
        depends_on=[grandparent_node.unique_id, another_grandparent_node.unique_id],
        file_path=SAMPLE_PROJ_PATH / "gen2/models/parent_sibling.sql",
        tags=["is_adopted"],
        config={"materialized": "view", "tags": ["is_adopted"]},
    )
    sample_nodes_with_parent_sibling = dict(sample_nodes)
    sample_nodes_with_parent_sibling[parent_sibling_node.unique_id] = parent_sibling_node
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes_with_parent_sibling, select=["+tag:is_child,+tag:is_adopted"]
    )
    expected = {
        grandparent_node.unique_id: grandparent_node,
        another_grandparent_node.unique_id: another_grandparent_node,
    }
    assert selected == expected


def test_select_nodes_by_tag_ancestry():
    parent_sibling_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.parent_sibling",
        resource_type=DbtResourceType.MODEL,
        depends_on=[grandparent_node.unique_id, another_grandparent_node.unique_id],
        file_path=SAMPLE_PROJ_PATH / "gen2/models/parent_sibling.sql",
        tags=["is_adopted"],
        config={"materialized": "view", "tags": ["is_adopted"]},
    )
    sample_nodes_with_parent_sibling = dict(sample_nodes)
    sample_nodes_with_parent_sibling[parent_sibling_node.unique_id] = parent_sibling_node
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes_with_parent_sibling, select=["+tag:is_adopted"]
    )
    expected = {
        grandparent_node.unique_id: grandparent_node,
        another_grandparent_node.unique_id: another_grandparent_node,
        parent_sibling_node.unique_id: parent_sibling_node,
    }
    assert selected == expected


def test_select_nodes_with_test_by_intersection_and_tag_ancestry():
    parent_sibling_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.parent_sibling",
        resource_type=DbtResourceType.MODEL,
        depends_on=[grandparent_node.unique_id, another_grandparent_node.unique_id],
        file_path="",
        tags=["is_adopted"],
        config={"materialized": "view", "tags": ["is_adopted"]},
    )
    test_node = DbtNode(
        unique_id=f"{DbtResourceType.TEST.value}.{SAMPLE_PROJ_PATH.stem}.test",
        resource_type=DbtResourceType.TEST,
        depends_on=[parent_node.unique_id, parent_sibling_node.unique_id],
        file_path="",
        config={},
    )
    new_sample_nodes = dict(sample_nodes)
    new_sample_nodes[parent_sibling_node.unique_id] = parent_sibling_node
    new_sample_nodes[test_node.unique_id] = test_node
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=new_sample_nodes, select=["+tag:has_child"])
    # Expected must not include `parent_sibling_node` nor `test_node`
    expected = {
        parent_node.unique_id: parent_node,
        grandparent_node.unique_id: grandparent_node,
        another_grandparent_node.unique_id: another_grandparent_node,
    }
    assert selected == expected


def test_select_nodes_by_select_path():
    # Path without star or graph selector
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["path:gen2/models"])
    expected = {
        parent_node.unique_id: parent_node,
    }
    assert selected == expected

    # Path with star
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["path:gen2/models/*"])
    expected = {
        parent_node.unique_id: parent_node,
    }
    assert selected == expected

    # Path with star and graph selector that retrieves descendants
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["path:gen2/models/*+"])
    expected = {
        child_node.unique_id: child_node,
        parent_node.unique_id: parent_node,
        sibling1_node.unique_id: sibling1_node,
        sibling2_node.unique_id: sibling2_node,
        sibling3_node.unique_id: sibling3_node,
    }
    assert selected == expected


def test_select_nodes_with_slash_but_no_path_selector():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["gen2/models"])
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
        sibling3_node.unique_id: sibling3_node,
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
        sibling3_node.unique_id: sibling3_node,
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
        sibling3_node.unique_id: sibling3_node,
    }
    assert selected == expected


def test_select_nodes_by_path_dir():
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["path:gen3/models"])
    expected = {
        child_node.unique_id: child_node,
        sibling1_node.unique_id: sibling1_node,
        sibling2_node.unique_id: sibling2_node,
        sibling3_node.unique_id: sibling3_node,
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
        "model.dbt-proj.public.sibling3",
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
        "model.dbt-proj.public.sibling3",
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
        "model.dbt-proj.public.sibling3",
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


def test_node_without_depends_on_with_tag_selector_should_not_raise_exception():
    standalone_test_node = DbtNode(
        unique_id=f"{DbtResourceType.TEST.value}.{SAMPLE_PROJ_PATH.stem}.standalone",
        resource_type=DbtResourceType.TEST,
        depends_on=[],
        tags=[],
        config={},
        file_path=SAMPLE_PROJ_PATH / "tests/generic/builtin.sql",
    )
    nodes = {standalone_test_node.unique_id: standalone_test_node}
    assert not select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=nodes, select=["tag:some-tag"])


def test_should_include_node_without_depends_on(selector_config):
    node = DbtNode(
        unique_id=f"{DbtResourceType.TEST.value}.{SAMPLE_PROJ_PATH.stem}.standalone",
        resource_type=DbtResourceType.TEST,
        depends_on=None,
        tags=[],
        config={},
        file_path=SAMPLE_PROJ_PATH / "tests/generic/builtin.sql",
    )
    selector = NodeSelector({}, selector_config)
    selector.visited_nodes = set()
    selector._should_include_node(node.unique_id, node)


@pytest.mark.parametrize(
    "select_statement, expected",
    [
        (
            ["+path:gen2/models"],
            [
                "model.dbt-proj.another_grandparent_node",
                "model.dbt-proj.grandparent",
                "model.dbt-proj.parent",
            ],
        ),
        (
            ["path:gen2/models+"],
            [
                "model.dbt-proj.child",
                "model.dbt-proj.parent",
                "model.dbt-proj.public.sibling3",
                "model.dbt-proj.sibling1",
                "model.dbt-proj.sibling2",
            ],
        ),
        (
            ["gen2/models+"],
            [
                "model.dbt-proj.child",
                "model.dbt-proj.parent",
                "model.dbt-proj.public.sibling3",
                "model.dbt-proj.sibling1",
                "model.dbt-proj.sibling2",
            ],
        ),
        (
            ["+gen2/models"],
            [
                "model.dbt-proj.another_grandparent_node",
                "model.dbt-proj.grandparent",
                "model.dbt-proj.parent",
            ],
        ),
        (
            ["1+tag:deprecated"],
            [
                "model.dbt-proj.parent",
                "model.dbt-proj.public.sibling3",
                "model.dbt-proj.sibling1",
                "model.dbt-proj.sibling2",
            ],
        ),
        (
            ["1+config.tags:deprecated"],
            [
                "model.dbt-proj.parent",
                "model.dbt-proj.public.sibling3",
                "model.dbt-proj.sibling1",
                "model.dbt-proj.sibling2",
            ],
        ),
        (
            ["config.materialized:table+"],
            [
                "model.dbt-proj.child",
                "model.dbt-proj.public.sibling3",
                "model.dbt-proj.sibling1",
                "model.dbt-proj.sibling2",
            ],
        ),
    ],
)
def test_select_using_graph_operators(select_statement, expected):
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=select_statement)
    assert sorted(selected.keys()) == expected


def test_select_nodes_by_at_operator():
    """Test basic @ operator selecting node, descendants and ancestors of all"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["@parent"])
    expected = [
        "model.dbt-proj.another_grandparent_node",
        "model.dbt-proj.child",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
        "model.dbt-proj.public.sibling3",
        "model.dbt-proj.sibling1",
        "model.dbt-proj.sibling2",
    ]
    assert sorted(selected.keys()) == expected


def test_select_nodes_by_at_operator_leaf_node():
    """Test @ operator on a leaf node (no descendants)"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["@child"])
    expected = [
        "model.dbt-proj.another_grandparent_node",
        "model.dbt-proj.child",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
    ]
    assert sorted(selected.keys()) == expected


def test_select_nodes_by_at_operator_root_node():
    """Test @ operator on a root node (no ancestors)"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["@grandparent"])
    expected = [
        "model.dbt-proj.another_grandparent_node",
        "model.dbt-proj.child",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
        "model.dbt-proj.public.sibling3",
        "model.dbt-proj.sibling1",
        "model.dbt-proj.sibling2",
    ]
    assert sorted(selected.keys()) == expected


def test_select_nodes_by_at_operator_union():
    """Test @ operator union with another selector"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["@child", "tag:has_child"])
    expected = [
        "model.dbt-proj.another_grandparent_node",
        "model.dbt-proj.child",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
    ]
    assert sorted(selected.keys()) == expected


def test_select_nodes_by_at_operator_with_path():
    """Test @ operator with a path"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["@gen2/models"])
    expected = [
        "model.dbt-proj.another_grandparent_node",
        "model.dbt-proj.child",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
        "model.dbt-proj.public.sibling3",
        "model.dbt-proj.sibling1",
        "model.dbt-proj.sibling2",
    ]
    assert sorted(selected.keys()) == expected


def test_select_nodes_by_at_operator_nonexistent_node():
    """Test @ operator with a node that doesn't exist"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["@nonexistent"])
    expected = []
    assert sorted(selected.keys()) == expected


def test_exclude_with_at_operator():
    """Test excluding nodes selected by @ operator"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["@parent"])
    expected = ["model.dbt-proj.orphaned"]
    assert sorted(selected.keys()) == expected


def test_select_nodes_with_period():
    """Test @ operator with a node that doesn't exist"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["public.sibling3"])
    expected = ["model.dbt-proj.public.sibling3"]
    assert sorted(selected.keys()) == expected


def test_exclude_nodes_with_period():
    """Test @ operator with a node that doesn't exist"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["public.sibling3"])
    expected = [
        "model.dbt-proj.another_grandparent_node",
        "model.dbt-proj.child",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.orphaned",
        "model.dbt-proj.parent",
        "model.dbt-proj.sibling1",
        "model.dbt-proj.sibling2",
    ]
    assert sorted(selected.keys()) == expected


def test_select_nodes_with_period_by_graph():
    """Test @ operator with a node that doesn't exist"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["+public.sibling3"])
    expected = [
        "model.dbt-proj.another_grandparent_node",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
        "model.dbt-proj.public.sibling3",
    ]
    assert sorted(selected.keys()) == expected


def test_exclude_nodes_with_period_by_graph():
    """Test @ operator with a node that doesn't exist"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["+public.sibling3"])
    expected = ["model.dbt-proj.child", "model.dbt-proj.orphaned", "model.dbt-proj.sibling1", "model.dbt-proj.sibling2"]
    assert sorted(selected.keys()) == expected


def test_select_nodes_with_period_with_at_operator():
    """Test @ operator with a node that doesn't exist"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, select=["@public.sibling3"])
    expected = [
        "model.dbt-proj.another_grandparent_node",
        "model.dbt-proj.grandparent",
        "model.dbt-proj.parent",
        "model.dbt-proj.public.sibling3",
    ]
    assert sorted(selected.keys()) == expected


def test_exclude_nodes_with_period_with_at_operator():
    """Test @ operator with a node that doesn't exist"""
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=sample_nodes, exclude=["@public.sibling3"])
    expected = ["model.dbt-proj.child", "model.dbt-proj.orphaned", "model.dbt-proj.sibling1", "model.dbt-proj.sibling2"]
    assert sorted(selected.keys()) == expected


def test_select_nodes_by_resource_type_source():
    """
    Test that 'resource_type:source' picks up only nodes with resource_type == SOURCE,
    excluding any models or other resource types.
    """
    local_nodes = dict(sample_nodes)
    source_node = DbtNode(
        unique_id=f"{DbtResourceType.SOURCE.value}.{SAMPLE_PROJ_PATH.stem}.my_source.my_table",
        resource_type=DbtResourceType.SOURCE,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "sources/my_source.yml",
        tags=[],
        config={},
    )

    local_nodes[source_node.unique_id] = source_node
    model_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.model_from_source",
        resource_type=DbtResourceType.MODEL,
        depends_on=[source_node.unique_id],
        file_path=SAMPLE_PROJ_PATH / "models/model_from_source.sql",
        tags=["depends_on_source"],
        config={"materialized": "table", "tags": ["depends_on_source"]},
    )

    local_nodes[model_node.unique_id] = model_node
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        select=["resource_type:source"],
    )
    expected = {
        source_node.unique_id: source_node,
    }
    assert selected == expected


def test_select_nodes_by_exclude_resource_type_model():
    """
    Test that 'exclude_resource_type:model' picks up only nodes with resource_type != MODEL,
    including any resources except models.
    """
    local_nodes = dict(sample_nodes)
    source_node = DbtNode(
        unique_id=f"{DbtResourceType.SOURCE.value}.{SAMPLE_PROJ_PATH.stem}.my_source.my_table",
        resource_type=DbtResourceType.SOURCE,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "sources/my_source.yml",
        tags=[],
        config={},
    )

    local_nodes[source_node.unique_id] = source_node
    model_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.{SAMPLE_PROJ_PATH.stem}.model_from_source",
        resource_type=DbtResourceType.MODEL,
        depends_on=[source_node.unique_id],
        file_path=SAMPLE_PROJ_PATH / "models/model_from_source.sql",
        tags=["depends_on_source"],
        config={"materialized": "table", "tags": ["depends_on_source"]},
    )

    local_nodes[model_node.unique_id] = model_node
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        select=["exclude_resource_type:model"],
    )

    assert source_node.unique_id in selected
    assert model_node.unique_id not in selected
    for model_id in sample_nodes.keys():
        assert model_id not in selected


def test_select_nodes_by_source_name():
    """
    Test selecting a single source node by exact name 'source:my_source.my_table'.
    The code in _should_include_node requires node.resource_type == SOURCE
    AND node.name == "my_source.my_table".
    """
    local_nodes = dict(sample_nodes)
    source_node = DbtNode(
        unique_id=f"{DbtResourceType.SOURCE.value}.{SAMPLE_PROJ_PATH.stem}.my_source.my_table",
        resource_type=DbtResourceType.SOURCE,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "sources/my_source.yml",
        tags=[],
        config={},
    )

    local_nodes[source_node.unique_id] = source_node
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        select=["source:my_source.my_table"],
    )
    expected = {source_node.unique_id: source_node}
    assert selected == expected


def test_exclude_nodes_by_resource_type_seed():
    """
    Test excluding any seed node via 'resource_type:seed'.
    """
    local_nodes = dict(sample_nodes)
    seed_node = DbtNode(
        unique_id=f"{DbtResourceType.SEED.value}.{SAMPLE_PROJ_PATH.stem}.my_seed",
        resource_type=DbtResourceType.SEED,
        depends_on=[],
        tags=[],
        config={},
        file_path=SAMPLE_PROJ_PATH / "seeds/seed.yml",
    )

    local_nodes[seed_node.unique_id] = seed_node
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=local_nodes, exclude=["resource_type:seed"])
    assert seed_node.unique_id not in selected
    for model_id in sample_nodes.keys():
        assert model_id in selected


def test_exclude_nodes_by_exclude_resource_type_seed():
    """
    Test keeping any seed node via 'exclude_resource_type:seed'.
    """
    local_nodes = dict(sample_nodes)
    seed_node = DbtNode(
        unique_id=f"{DbtResourceType.SEED.value}.{SAMPLE_PROJ_PATH.stem}.my_seed",
        resource_type=DbtResourceType.SEED,
        depends_on=[],
        tags=[],
        config={},
        file_path=SAMPLE_PROJ_PATH / "models/my_seed.yml",
    )

    local_nodes[seed_node.unique_id] = seed_node
    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=local_nodes, exclude=["exclude_resource_type:seed"])
    assert seed_node.unique_id in selected
    for model_id in sample_nodes.keys():
        assert model_id not in selected


def test_source_selector():
    """
    Covers:
    1) source_selection = self.node_name[len(SOURCE_SELECTOR):]
    2) root_nodes.update(...) in that source logic
    3) __repr__ for SelectorConfig
    4) the line 'if node.resource_name not in self.config.sources: return False'
    """
    local_nodes = dict(sample_nodes)

    source_node_match = DbtNode(
        unique_id=f"{DbtResourceType.SOURCE.value}.{SAMPLE_PROJ_PATH.stem}.my_source",
        resource_type=DbtResourceType.SOURCE,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "sources/my_source.yml",
        tags=[],
        config={},
    )
    source_node_mismatch = DbtNode(
        unique_id=f"{DbtResourceType.SOURCE.value}.{SAMPLE_PROJ_PATH.stem}.another_source",
        resource_type=DbtResourceType.SOURCE,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "sources/another_source.yml",
        tags=[],
        config={},
    )
    local_nodes[source_node_match.unique_id] = source_node_match
    local_nodes[source_node_mismatch.unique_id] = source_node_mismatch

    select_statement = ["source:my_source"]

    config = SelectorConfig(SAMPLE_PROJ_PATH, select_statement[0])

    config_repr = repr(config)
    assert "my_source" in config_repr, "Expected 'my_source' to appear in the config repr"

    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=local_nodes, select=select_statement)

    expected = {
        source_node_match.unique_id: source_node_match,
    }
    assert selected == expected, f"Expected only {source_node_match.unique_id} to match"
