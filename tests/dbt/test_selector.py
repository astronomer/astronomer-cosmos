from pathlib import Path

import pytest

from cosmos.constants import DbtResourceType
from cosmos.dbt.graph import DbtNode
from cosmos.dbt.selector import NodeSelector, SelectorConfig, YamlSelectors, select_nodes
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


def test_select_nodes_by_invalid_config(caplog):
    select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=sample_nodes,
        select=["config.invalid_config:test+"],
    )
    assert "Unsupported config key selector: invalid_config" in caplog.messages


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


def test_select_nodes_by_exposure_name():
    """
    Test selecting a single exposure node by exact name 'exposure:exposure_name'.
    The code in _should_include_node requires node.resource_type == EXPOSURE
    AND node.name == "exposure_name".
    """
    local_nodes = dict(sample_nodes)
    exposure_node = DbtNode(
        unique_id=f"{DbtResourceType.EXPOSURE.value}.{SAMPLE_PROJ_PATH.stem}.exposure_name",
        resource_type=DbtResourceType.EXPOSURE,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "exposures/my_exposure.yml",
        tags=[],
        config={},
    )

    local_nodes[exposure_node.unique_id] = exposure_node
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        select=["exposure:exposure_name"],
    )
    expected = {exposure_node.unique_id: exposure_node}
    assert selected == expected


def test_select_nodes_by_select_package():
    """
    Test selecting nodes by package name using 'package:package_name'.
    Used when loading from manifest to include only nodes from a given package.
    """
    local_nodes = dict(sample_nodes)
    # Add nodes from dbt_artifacts package (package_name is set when loading from manifest)
    artifact_node_1 = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.dbt_artifacts.artifact_model_1",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "dbt_packages/dbt_artifacts/models/artifact_model_1.sql",
        tags=[],
        config={},
        package_name="dbt_artifacts",
    )
    artifact_node_2 = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.dbt_artifacts.artifact_model_2",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "dbt_packages/dbt_artifacts/models/artifact_model_2.sql",
        tags=[],
        config={},
        package_name="dbt_artifacts",
    )
    local_nodes[artifact_node_1.unique_id] = artifact_node_1
    local_nodes[artifact_node_2.unique_id] = artifact_node_2

    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        select=["package:dbt_artifacts"],
    )
    expected = {artifact_node_1.unique_id: artifact_node_1, artifact_node_2.unique_id: artifact_node_2}
    assert selected == expected


def test_select_nodes_by_exclude_package():
    """
    Test excluding nodes by package name using 'package:package_name'.
    This fixes manifest load mode not respecting exclude for packages (e.g. dbt_artifacts).
    """
    local_nodes = dict(sample_nodes)
    # Add nodes from dbt_artifacts package
    artifact_node_1 = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.dbt_artifacts.artifact_model_1",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "dbt_packages/dbt_artifacts/models/artifact_model_1.sql",
        tags=[],
        config={},
        package_name="dbt_artifacts",
    )
    local_nodes[artifact_node_1.unique_id] = artifact_node_1

    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        exclude=["package:dbt_artifacts"],
    )
    # All original sample_nodes remain; artifact nodes are excluded
    assert artifact_node_1.unique_id not in selected
    assert set(selected.keys()) == set(sample_nodes.keys())


def test_select_nodes_by_exclude_bare_package_name():
    """
    Bare package name (no 'package:' prefix) is equivalent to 'package:name', matching dbt behavior.
    So exclude=['dbt_artifacts'] works the same as exclude=['package:dbt_artifacts'].
    """
    local_nodes = dict(sample_nodes)
    artifact_node = DbtNode(
        unique_id=f"{DbtResourceType.MODEL.value}.dbt_artifacts.artifact_model_1",
        resource_type=DbtResourceType.MODEL,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "dbt_packages/dbt_artifacts/models/artifact_model_1.sql",
        tags=[],
        config={},
        package_name="dbt_artifacts",
    )
    local_nodes[artifact_node.unique_id] = artifact_node

    selected_bare = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        exclude=["dbt_artifacts"],
    )
    selected_explicit = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        exclude=["package:dbt_artifacts"],
    )
    assert artifact_node.unique_id not in selected_bare
    assert artifact_node.unique_id not in selected_explicit
    assert set(selected_bare.keys()) == set(selected_explicit.keys()) == set(sample_nodes.keys())


def test_select_nodes_by_bare_folder_name():
    """
    Bare identifier as folder name (path segment) matches nodes under that folder.
    Ensures select=['folder_a'] / exclude=['folder_a'] work with manifest load mode.
    """
    # gen1 contains grandparent, another_grandparent; gen3 contains child, sibling1, sibling2, sibling3, orphaned
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=sample_nodes,
        select=["gen1"],
    )
    assert set(selected.keys()) == {grandparent_node.unique_id, another_grandparent_node.unique_id}


def test_exclude_nodes_by_bare_folder_name():
    """Exclude by folder name (path segment) excludes all nodes under that folder."""
    excluded = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=sample_nodes,
        exclude=["gen3"],
    )
    gen3_ids = {
        child_node.unique_id,
        sibling1_node.unique_id,
        sibling2_node.unique_id,
        sibling3_node.unique_id,
        orphaned_node.unique_id,
    }
    assert gen3_ids.isdisjoint(excluded.keys())
    assert grandparent_node.unique_id in excluded
    assert parent_node.unique_id in excluded


def test_select_exposure_nodes_by_graph_ancestry():
    """
    Test selecting an exposure node and its directs ancestors using the syntax '+exposure:exposure_name'.
    """

    local_nodes = dict(sample_nodes)
    exposure_node = DbtNode(
        unique_id=f"{DbtResourceType.EXPOSURE.value}.{SAMPLE_PROJ_PATH.stem}.exposure_name",
        resource_type=DbtResourceType.EXPOSURE,
        depends_on=[parent_node.unique_id],
        file_path=SAMPLE_PROJ_PATH / "exposures/my_exposure.yml",
        tags=[],
        config={},
    )

    local_nodes[exposure_node.unique_id] = exposure_node
    selected = select_nodes(
        project_dir=SAMPLE_PROJ_PATH,
        nodes=local_nodes,
        select=["+exposure:exposure_name"],
    )
    expected = {
        exposure_node.unique_id: exposure_node,
        parent_node.unique_id: parent_node,
        grandparent_node.unique_id: grandparent_node,
        another_grandparent_node.unique_id: another_grandparent_node,
    }
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


def test_exposure_selector():
    """
    Covers:
    1) exposure_selection = self.node_name[len(EXPOSURE_SELECTOR):]
    2) root_nodes.update(...) in that exposure logic
    3) __repr__ for SelectorConfig
    4) the line 'if node.resource_name not in self.config.exposures: return False'
    """
    local_nodes = dict(sample_nodes)

    exposure_node_match = DbtNode(
        unique_id=f"{DbtResourceType.EXPOSURE.value}.{SAMPLE_PROJ_PATH.stem}.my_exposure",
        resource_type=DbtResourceType.EXPOSURE,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "exposures/my_exposure.yml",
        tags=[],
        config={},
    )
    exposure_node_mismatch = DbtNode(
        unique_id=f"{DbtResourceType.EXPOSURE.value}.{SAMPLE_PROJ_PATH.stem}.another_exposure",
        resource_type=DbtResourceType.EXPOSURE,
        depends_on=[],
        file_path=SAMPLE_PROJ_PATH / "exposures/another_exposure.yml",
        tags=[],
        config={},
    )
    local_nodes[exposure_node_match.unique_id] = exposure_node_match
    local_nodes[exposure_node_mismatch.unique_id] = exposure_node_mismatch

    select_statement = ["exposure:my_exposure"]

    config = SelectorConfig(SAMPLE_PROJ_PATH, select_statement[0])

    config_repr = repr(config)
    assert "my_exposure" in config_repr, "Expected 'my_exposure' to appear in the config repr"

    selected = select_nodes(project_dir=SAMPLE_PROJ_PATH, nodes=local_nodes, select=select_statement)

    expected = {
        exposure_node_match.unique_id: exposure_node_match,
    }
    assert selected == expected, f"Expected only {exposure_node_match.unique_id} to match"


@pytest.mark.parametrize(
    "selector_name, selector_definition, parsed_selector",
    [
        (
            "path_method",
            {"name": "path_method", "definition": {"method": "path", "value": "models/staging"}},
            {"select": ["path:models/staging"], "exclude": None},
        ),
        (
            "tag_method",
            {"name": "tag_method", "definition": {"method": "tag", "value": "nightly"}},
            {"select": ["tag:nightly"], "exclude": None},
        ),
        (
            "fqn_method",
            {"name": "fqn_method", "definition": {"method": "fqn", "value": "customers"}},
            {"select": ["customers"], "exclude": None},
        ),
        (
            "fqn_star_method",
            {"name": "fqn_star_method", "definition": {"method": "fqn", "value": "*"}},
            {"select": None, "exclude": None},
        ),
        (
            "config_method_materialized",
            {
                "name": "config_method_materialized",
                "definition": {"method": "config.materialized", "value": "incremental"},
            },
            {"select": ["config.materialized:incremental"], "exclude": None},
        ),
        (
            "config_method_schema",
            {"name": "config_method_schema", "definition": {"method": "config.schema", "value": "audit"}},
            {"select": ["config.schema:audit"], "exclude": None},
        ),
        (
            "config_method_tags",
            {"name": "config_method_tags", "definition": {"method": "config.tags", "value": "nightly"}},
            {"select": ["config.tags:nightly"], "exclude": None},
        ),
        (
            "config_method_meta",
            {"name": "config_method_meta", "definition": {"method": "config.meta.allow_pii", "value": "true"}},
            {"select": ["config.meta.allow_pii:true"], "exclude": None},
        ),
        (
            "source_method",
            {"name": "source_method", "definition": {"method": "source", "value": "raw_*"}},
            {"select": ["source:raw_*"], "exclude": None},
        ),
        (
            "exposure_method",
            {"name": "exposure_method", "definition": {"method": "exposure", "value": "daily_report"}},
            {"select": ["exposure:daily_report"], "exclude": None},
        ),
        (
            "resource_type_selector_method",
            {"name": "resource_type_selector_method", "definition": {"method": "resource_type", "value": "source"}},
            {"select": ["resource_type:source"], "exclude": None},
        ),
        (
            "exclude_resource_type_selector_method",
            {
                "name": "exclude_resource_type_selector_method",
                "definition": {"method": "exclude_resource_type", "value": "model"},
            },
            {"select": ["exclude_resource_type:model"], "exclude": None},
        ),
        (
            "intersection_method",
            {
                "name": "intersection_method",
                "definition": {
                    "intersection": [
                        {"method": "tag", "value": "nightly"},
                        {"method": "config.materialized", "value": "table"},
                    ]
                },
            },
            {"select": ["tag:nightly,config.materialized:table"], "exclude": None},
        ),
        (
            "union_method",
            {
                "name": "union_method",
                "definition": {
                    "union": [{"method": "tag", "value": "nightly"}, {"method": "config.materialized", "value": "view"}]
                },
            },
            {"select": ["tag:nightly", "config.materialized:view"], "exclude": None},
        ),
        (
            "exclude_method",
            {
                "name": "exclude_method",
                "definition": {"method": "tag", "value": "", "exclude": [{"method": "tag", "value": "deprecated"}]},
            },
            {"select": ["tag:"], "exclude": ["tag:deprecated"]},
        ),
        (
            "complex_method",
            {
                "name": "complex_method",
                "definition": {
                    "union": [
                        {"method": "tag", "value": "nightly"},
                        {"method": "path", "value": "models/staging"},
                        {"exclude": [{"method": "config.materialized", "value": "ephemeral"}]},
                    ]
                },
            },
            {"select": ["tag:nightly", "path:models/staging"], "exclude": ["config.materialized:ephemeral"]},
        ),
    ],
)
def test_valid_method_yaml_selectors(selector_name, selector_definition, parsed_selector):
    selectors = {selector_name: selector_definition}
    yaml_selectors = YamlSelectors.parse(selectors)

    result = yaml_selectors.get_parsed(selector_name)

    assert result == parsed_selector


@pytest.mark.parametrize(
    "selector_name, selector_definition, parsed_selector",
    [
        (
            "with_parents_no_children",
            {"name": "with_parents_no_children", "definition": {"method": "tag", "value": "nightly", "parents": True}},
            {"select": ["+tag:nightly"], "exclude": None},
        ),
        (
            "with_children_no_parents",
            {"name": "with_children_no_parents", "definition": {"method": "tag", "value": "nightly", "children": True}},
            {"select": ["tag:nightly+"], "exclude": None},
        ),
        (
            "with_parents_and_children",
            {
                "name": "with_parents_and_children",
                "definition": {"method": "tag", "value": "nightly", "parents": True, "children": True},
            },
            {"select": ["+tag:nightly+"], "exclude": None},
        ),
        (
            "with_parents_depth",
            {
                "name": "with_parents_depth",
                "definition": {"method": "tag", "value": "nightly", "parents": True, "parents_depth": 2},
            },
            {"select": ["2+tag:nightly"], "exclude": None},
        ),
        (
            "with_parents_depth_and_children",
            {
                "name": "with_parents_depth_and_children",
                "definition": {
                    "method": "tag",
                    "value": "nightly",
                    "parents": True,
                    "parents_depth": 2,
                    "children": True,
                },
            },
            {"select": ["2+tag:nightly+"], "exclude": None},
        ),
        (
            "with_children_depth",
            {
                "name": "with_children_depth",
                "definition": {"method": "tag", "value": "nightly", "children": True, "children_depth": 3},
            },
            {"select": ["tag:nightly+3"], "exclude": None},
        ),
        (
            "with_children_depth_and_parents",
            {
                "name": "with_children_depth_and_parents",
                "definition": {
                    "method": "tag",
                    "value": "nightly",
                    "parents": True,
                    "children": True,
                    "children_depth": 3,
                },
            },
            {"select": ["+tag:nightly+3"], "exclude": None},
        ),
        (
            "with_parents_depth_and_children_depth",
            {
                "name": "with_parents_depth_and_children_depth",
                "definition": {
                    "method": "tag",
                    "value": "nightly",
                    "parents": True,
                    "parents_depth": 2,
                    "children": True,
                    "children_depth": 3,
                },
            },
            {"select": ["2+tag:nightly+3"], "exclude": None},
        ),
        (
            "with_childrens_parents",
            {
                "name": "with_childrens_parents",
                "definition": {"method": "tag", "value": "nightly", "childrens_parents": True},
            },
            {"select": ["@tag:nightly"], "exclude": None},
        ),
    ],
)
def test_valid_graph_operator_yaml_selectors(selector_name, selector_definition, parsed_selector):
    selectors = {selector_name: selector_definition}
    yaml_selectors = YamlSelectors.parse(selectors)

    result = yaml_selectors.get_parsed(selector_name)

    assert result == parsed_selector


@pytest.mark.parametrize(
    "selector_name, selector_definition, exception_msg",
    [
        # Node selector methods supported by dbt but not by Cosmos filter selection
        (
            "access_method",
            {"name": "access_method", "definition": {"method": "access", "value": "public"}},
            "Unsupported selector method: 'access'",
        ),
        (
            "file_method",
            {"name": "file_method", "definition": {"method": "file", "value": "my_model.sql"}},
            "Unsupported selector method: 'file'",
        ),
        (
            "group_method",
            {"name": "group_method", "definition": {"method": "group", "value": "finance"}},
            "Unsupported selector method: 'group'",
        ),
        (
            "metric_method",
            {"name": "metric_method", "definition": {"method": "metric", "value": "revenue"}},
            "Unsupported selector method: 'metric'",
        ),
        (
            "package_method",
            {"name": "package_method", "definition": {"method": "package", "value": "dbt_utils"}},
            "Unsupported selector method: 'package'",
        ),
        (
            "result_method",
            {"name": "result_method", "definition": {"method": "result", "value": "error"}},
            "Unsupported selector method: 'result'",
        ),
        (
            "saved_query_method",
            {"name": "saved_query_method", "definition": {"method": "saved_query", "value": "quarterly_report"}},
            "Unsupported selector method: 'saved_query'",
        ),
        (
            "semantic_model_method",
            {"name": "semantic_model_method", "definition": {"method": "semantic_model", "value": "customer_metrics"}},
            "Unsupported selector method: 'semantic_model'",
        ),
        (
            "source_status_method",
            {"name": "source_status_method", "definition": {"method": "source_status", "value": "fresher"}},
            "Unsupported selector method: 'source_status'",
        ),
        (
            "state_method",
            {"name": "state_method", "definition": {"method": "state", "value": "modified"}},
            "Unsupported selector method: 'state'",
        ),
        (
            "test_name_method",
            {"name": "test_name_method", "definition": {"method": "test_name", "value": "unique_id"}},
            "Unsupported selector method: 'test_name'",
        ),
        (
            "test_type_method",
            {"name": "test_type_method", "definition": {"method": "test_type", "value": "generic"}},
            "Unsupported selector method: 'test_type'",
        ),
        (
            "unit_test_method",
            {"name": "unit_test_method", "definition": {"method": "unit_test", "value": "test_my_model"}},
            "Unsupported selector method: 'unit_test'",
        ),
        (
            "version_method",
            {"name": "version_method", "definition": {"method": "version", "value": "latest"}},
            "Unsupported selector method: 'version'",
        ),
    ],
)
def test_invalid_cosmos_method_yaml_selectors(selector_name, selector_definition, exception_msg):
    selectors = {selector_name: selector_definition}

    with pytest.raises(CosmosValueError) as err_info:
        _ = YamlSelectors.parse(selectors)

    assert err_info.value.args[0] == exception_msg


@pytest.mark.parametrize(
    "selector_name, selector_definition, exception_msg",
    [
        (
            "multi_root_union",
            {
                "name": "multi_root_union",
                "definition": {
                    "union": [{"method": "tag", "value": "nightly"}],
                    "intersection": [{"method": "tag", "value": "daily"}],
                },
            },
            "Only a single 'union' or 'intersection' key is allowed in a root level selector definition; found union,intersection.",
        ),
        (
            "multi_root_intersection",
            {
                "name": "multi_root_intersection",
                "definition": {
                    "intersection": [{"method": "tag", "value": "nightly"}],
                    "union": [{"method": "tag", "value": "daily"}],
                },
            },
            "Only a single 'union' or 'intersection' key is allowed in a root level selector definition; found intersection,union.",
        ),
        (
            "cli_selector",
            {"name": "cli_selector", "definition": "tag:nightly"},
            "Expected to find union, intersection, or method definition, instead found <class 'str'>: tag:nightly",
        ),
        (
            "key_value_selector",
            {"name": "key_value_selector", "definition": {"tag": "nightly"}},
            "Expected to find union, intersection, or method definition, instead found <class 'dict'>: {'tag': 'nightly'}",
        ),
        (
            "circular_reference_selector",
            {
                "name": "circular_reference_selector",
                "definition": {"method": "selector", "value": "circular_reference_selector"},
            },
            "Existing selector definition for circular_reference_selector not found.",
        ),
        (
            "invalid_selector_keys",
            {"name": "invalid_selector_keys", "definition": {"invalid_key": "invalid_value"}},
            "Expected to find union, intersection, or method definition, instead found <class 'dict'>: {'invalid_key': 'invalid_value'}",
        ),
        (
            "missing_method_key",
            {"name": "missing_method_key", "definition": {"value": "nightly"}},
            "Expected to find union, intersection, or method definition, instead found <class 'dict'>: {'value': 'nightly'}",
        ),
        (
            "missing_value_key",
            {"name": "missing_value_key", "definition": {"method": "tag"}},
            "Expected 'method' and 'value' keys, but got ['method']",
        ),
        (
            "missing_union_key",
            {"name": "missing_union_key", "definition": {"union": {"method": "tag"}}},
            "Invalid value for key 'union'. Expected a list.",
        ),
        (
            "non_int_parents_depth",
            {
                "name": "non_int_parents_depth",
                "definition": {"method": "tag", "value": "nightly", "parents": True, "parents_depth": "two"},
            },
            "parents_depth must be an integer, got <class 'str'>: two",
        ),
        (
            "non_int_children_depth",
            {
                "name": "non_int_children_depth",
                "definition": {"method": "tag", "value": "nightly", "children": True, "children_depth": "three"},
            },
            "children_depth must be an integer, got <class 'str'>: three",
        ),
        (
            "childrens_parents_and_parents_depth",
            {
                "name": "childrens_parents_and_parents_depth",
                "definition": {"method": "tag", "value": "nightly", "childrens_parents": True, "parents_depth": 2},
            },
            "childrens_parents cannot be combined with parents_depth or children_depth.",
        ),
        (
            "childrens_parents_and_children_depth",
            {
                "name": "childrens_parents_and_children_depth",
                "definition": {"method": "tag", "value": "nightly", "childrens_parents": True, "children_depth": 2},
            },
            "childrens_parents cannot be combined with parents_depth or children_depth.",
        ),
        (
            "multiple_excludes_in_union",
            {
                "name": "multiple_excludes_in_union",
                "definition": {
                    "union": [
                        {"method": "tag", "value": "nightly", "exclude": [{"method": "tag", "value": "deprecated"}]},
                        {"method": "path", "value": "models/", "exclude": [{"method": "tag", "value": "archived"}]},
                    ]
                },
            },
            "You cannot provide multiple exclude arguments to the same selector set operator:\nexclude:\n- method: tag\n  value: archived\nmethod: path\nvalue: models/\n",
        ),
        (
            "unknown_random_method",
            {"name": "unknown_random_method", "definition": {"method": "unknown_random", "value": "test"}},
            "Unsupported selector method: 'unknown_random'",
        ),
        (
            "non_dict_definition",
            "not_a_dict",
            "Invalid selector definition for 'non_dict_definition'. Expected a dict, got <class 'str'>: not_a_dict",
        ),
        (
            "missing_name_key",
            {"definition": {"method": "tag", "value": "nightly"}},
            "Selector definition for 'missing_name_key' must contain 'name' and 'definition' keys.",
        ),
        (
            "missing_definition_key",
            {"name": "missing_definition_key"},
            "Selector definition for 'missing_definition_key' must contain 'name' and 'definition' keys.",
        ),
    ],
)
def test_invalid_dbt_method_yaml_selectors(selector_name, selector_definition, exception_msg):
    selectors = {selector_name: selector_definition}

    with pytest.raises(CosmosValueError) as err_info:
        _ = YamlSelectors.parse(selectors)

    assert err_info.value.args[0] == exception_msg


@pytest.mark.parametrize(
    "selector_name, definition_key, invalid_value, expected_error_fragment",
    [
        ("union_not_a_list_string", "union", "not_a_list", "Invalid value for key 'union'. Expected a list."),
        ("intersection_not_a_list_int", "intersection", 42, "Invalid value for key 'intersection'. Expected a list."),
        ("union_not_a_list_dict", "union", {"nested": "dict"}, "Invalid value for key 'union'. Expected a list."),
    ],
)
def test_invalid_value_type_for_list_keys(selector_name, definition_key, invalid_value, expected_error_fragment):
    selectors = {selector_name: {"name": selector_name, "definition": {definition_key: invalid_value}}}

    with pytest.raises(CosmosValueError) as err_info:
        _ = YamlSelectors.parse(selectors)

    assert expected_error_fragment in err_info.value.args[0]


@pytest.mark.parametrize(
    "selector_name, definition_key, invalid_list_item, item_type_str",
    [
        ("string_in_union_list", "union", "tag:daily", "<class 'str'>"),
        ("integer_in_union_list", "union", 42, "<class 'int'>"),
        ("none_in_intersection_list", "intersection", None, "<class 'NoneType'>"),
        ("boolean_in_union_list", "union", True, "<class 'bool'>"),
        ("list_in_union_list", "union", ["nested", "list"], "<class 'list'>"),
        ("float_in_intersection_list", "intersection", 3.14, "<class 'float'>"),
    ],
)
def test_invalid_items_in_selector_lists(selector_name, definition_key, invalid_list_item, item_type_str):
    selectors = {
        selector_name: {
            "name": selector_name,
            "definition": {definition_key: [{"method": "tag", "value": "nightly"}, invalid_list_item]},
        }
    }

    with pytest.raises(CosmosValueError) as err_info:
        _ = YamlSelectors.parse(selectors)

    assert (
        f'Invalid value type {item_type_str} in key "{definition_key}", expected dict (value: {invalid_list_item}).'
        in err_info.value.args[0]
    )


@pytest.mark.parametrize(
    "selector_name, definition_key, non_string_key, key_type_str",
    [
        ("int_key_in_union", "union", 123, "<class 'int'>"),
        ("float_key_in_intersection", "intersection", 3.14, "<class 'float'>"),
        ("tuple_key_in_union", "union", (1, 2), "<class 'tuple'>"),
        ("bool_key_in_intersection", "intersection", True, "<class 'bool'>"),
        ("none_key_in_union", "union", None, "<class 'NoneType'>"),
    ],
)
def test_non_string_keys_in_selector_dicts(selector_name, definition_key, non_string_key, key_type_str):
    selectors = {
        selector_name: {
            "name": selector_name,
            "definition": {definition_key: [{non_string_key: "value", "method": "tag", "value": "nightly"}]},
        }
    }

    with pytest.raises(CosmosValueError) as err_info:
        _ = YamlSelectors.parse(selectors)

    assert f'Expected all keys to "{definition_key}" dict to be strings' in err_info.value.args[0]
    assert f'but "{non_string_key}" is a "{key_type_str}"' in err_info.value.args[0]


def test_selector_reference_resolves_from_cache():
    selectors = {
        "base_selector": {"name": "base_selector", "definition": {"method": "tag", "value": "nightly"}},
        "reference_selector": {
            "name": "reference_selector",
            "definition": {"method": "selector", "value": "base_selector"},
        },
    }

    yaml_selectors = YamlSelectors.parse(selectors)

    base_result = yaml_selectors.get_parsed("base_selector")
    reference_result = yaml_selectors.get_parsed("reference_selector")

    assert base_result == {"select": ["tag:nightly"], "exclude": None}
    assert reference_result == base_result
