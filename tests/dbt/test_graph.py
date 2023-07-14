from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos.dbt.graph import DbtGraph, LoadMode, CosmosLoadDbtException
from cosmos.dbt.project import DbtProject

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"
SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"


def test_load_via_manifest_with_exclude():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path=SAMPLE_MANIFEST)
    dbt_graph = DbtGraph(project=dbt_project, exclude=["config.materialized:table"])
    dbt_graph.load_from_dbt_manifest()

    assert len(dbt_graph.nodes) == 28
    assert len(dbt_graph.filtered_nodes) == 26
    assert "model.jaffle_shop.orders" not in dbt_graph.filtered_nodes

    sample_node = dbt_graph.nodes["model.jaffle_shop.customers"]
    assert sample_node.name == "customers"
    assert sample_node.unique_id == "model.jaffle_shop.customers"
    assert sample_node.resource_type == "model"
    assert sample_node.depends_on == [
        "model.jaffle_shop.stg_customers",
        "model.jaffle_shop.stg_orders",
        "model.jaffle_shop.stg_payments",
    ]
    assert sample_node.file_path == DBT_PROJECTS_ROOT_DIR / "jaffle_shop/models/customers.sql"


@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
def test_load_automatic_manifest_is_available(mock_load_from_dbt_manifest):
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path=SAMPLE_MANIFEST)
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load(execution_mode="local")
    assert mock_load_from_dbt_manifest.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", side_effect=FileNotFoundError())
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", return_value=None)
def test_load_automatic_without_manifest(mock_load_via_dbt_ls, mock_load_via_custom_parser):
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path="/tmp/manifest.json")
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load(execution_mode="local")
    assert mock_load_via_dbt_ls.called
    assert not mock_load_via_custom_parser.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", side_effect=FileNotFoundError())
def test_load_automatic_without_manifest_and_without_dbt_cmd(mock_load_via_dbt_ls, mock_load_via_custom_parser):
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load(execution_mode="local", method=LoadMode.AUTOMATIC)
    assert mock_load_via_dbt_ls.called
    assert mock_load_via_custom_parser.called


def test_load_manifest_without_manifest():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(project=dbt_project)
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load(execution_mode="local", method=LoadMode.DBT_MANIFEST)
    assert err_info.value.args[0] == "Unable to load manifest using None"


@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
def test_load_manifest_with_manifest(mock_load_from_dbt_manifest):
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path=SAMPLE_MANIFEST)
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load(execution_mode="local", method=LoadMode.DBT_MANIFEST)
    assert mock_load_from_dbt_manifest.called


@pytest.mark.parametrize(
    "exec_mode,method,expected_function",
    [
        ("local", LoadMode.AUTOMATIC, "mock_load_via_dbt_ls"),
        ("virtualenv", LoadMode.AUTOMATIC, "mock_load_via_dbt_ls"),
        ("kubernetes", LoadMode.AUTOMATIC, "mock_load_via_custom_parser"),
        ("docker", LoadMode.AUTOMATIC, "mock_load_via_custom_parser"),
        ("local", LoadMode.DBT_LS, "mock_load_via_dbt_ls"),
        ("local", LoadMode.CUSTOM, "mock_load_via_custom_parser"),
    ],
)
@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
def test_load(
    mock_load_from_dbt_manifest, mock_load_via_dbt_ls, mock_load_via_custom_parser, exec_mode, method, expected_function
):
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(project=dbt_project)

    dbt_graph.load(method=method, execution_mode=exec_mode)
    load_function = locals()[expected_function]
    assert load_function.called


@pytest.mark.integration
def test_load_via_dbt_ls_with_exclude():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(project=dbt_project, select=["*customers*"], exclude=["*orders*"])
    dbt_graph.load_via_dbt_ls()
    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == 7
    expected_keys = [
        "model.jaffle_shop.customers",
        "model.jaffle_shop.stg_customers",
        "seed.jaffle_shop.raw_customers",
        "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
        "test.jaffle_shop.not_null_stg_customers_customer_id.e2cfb1f9aa",
        "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1",
        "test.jaffle_shop.unique_stg_customers_customer_id.c7614daada",
    ]
    assert list(dbt_graph.nodes.keys()) == expected_keys

    sample_node = dbt_graph.nodes["model.jaffle_shop.customers"]
    assert sample_node.name == "customers"
    assert sample_node.unique_id == "model.jaffle_shop.customers"
    assert sample_node.resource_type == "model"
    assert sample_node.depends_on == [
        "model.jaffle_shop.stg_customers",
        "model.jaffle_shop.stg_orders",
        "model.jaffle_shop.stg_payments",
    ]
    assert sample_node.file_path == DBT_PROJECTS_ROOT_DIR / "jaffle_shop/models/customers.sql"


@pytest.mark.integration
def test_load_via_dbt_ls_without_exclude():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load_via_dbt_ls()

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == 28


def test_load_via_load_via_custom_parser():
    dbt_project = DbtProject(
        name="jaffle_shop",
        root_dir=DBT_PROJECTS_ROOT_DIR,
    )
    dbt_graph = DbtGraph(project=dbt_project)

    dbt_graph.load_via_custom_parser()

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    # the custom parser does not add dbt test nodes
    assert len(dbt_graph.nodes) == 8
