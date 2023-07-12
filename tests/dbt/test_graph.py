from pathlib import Path

import pytest

from cosmos.dbt.graph import DbtGraph
from cosmos.dbt.project import DbtProject

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"
SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"


def test_load_via_manifest_with_exclude():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest=SAMPLE_MANIFEST)
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
