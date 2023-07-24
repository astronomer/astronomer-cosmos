from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos.constants import ExecutionMode, DbtResourceType
from cosmos.dbt.graph import DbtGraph, LoadMode, CosmosLoadDbtException
from cosmos.config import CosmosConfig, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.exceptions import CosmosValueError

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"
SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"
SAMPLE_MANIFEST_PY = Path(__file__).parent.parent / "sample/manifest_python.json"


@pytest.mark.parametrize(
    "pipeline_name,manifest_filepath,model_filepath",
    [("jaffle_shop", SAMPLE_MANIFEST, "customers.sql"), ("jaffle_shop_python", SAMPLE_MANIFEST_PY, "customers.py")],
)
def test_load_via_manifest_with_exclude(pipeline_name, manifest_filepath, model_filepath):
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / pipeline_name,
                manifest=manifest_filepath,
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                exclude=["config.materialized:table"],
            ),
        ),
    )
    dbt_graph.load_from_dbt_manifest()

    assert len(dbt_graph.nodes) == 28
    assert len(dbt_graph.filtered_nodes) == 26
    assert "model.jaffle_shop.orders" not in dbt_graph.filtered_nodes

    sample_node = dbt_graph.nodes["model.jaffle_shop.customers"]
    assert sample_node.name == "customers"
    assert sample_node.unique_id == "model.jaffle_shop.customers"
    assert sample_node.resource_type == DbtResourceType.MODEL
    assert sample_node.depends_on == [
        "model.jaffle_shop.stg_customers",
        "model.jaffle_shop.stg_orders",
        "model.jaffle_shop.stg_payments",
    ]
    assert sample_node.file_path == DBT_PROJECTS_ROOT_DIR / f"{pipeline_name}/models/{model_filepath}"


@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
def test_load_automatic_manifest_is_available(mock_load_from_dbt_manifest):
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / "jaffle_shop",
                manifest=SAMPLE_MANIFEST,
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                exclude=["config.materialized:table"],
            ),
        ),
    )
    dbt_graph.load()
    assert mock_load_from_dbt_manifest.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", side_effect=FileNotFoundError())
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", return_value=None)
def test_load_automatic_without_manifest(mock_load_via_dbt_ls, mock_load_via_custom_parser):
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / "jaffle_shop",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                exclude=["config.materialized:table"],
            ),
        ),
    )
    dbt_graph.load()
    assert mock_load_via_dbt_ls.called
    assert not mock_load_via_custom_parser.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", side_effect=FileNotFoundError())
def test_load_automatic_without_manifest_and_without_dbt_cmd(mock_load_via_dbt_ls, mock_load_via_custom_parser):
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / "jaffle_shop",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                exclude=["config.materialized:table"],
            ),
        ),
    )
    dbt_graph.load()
    assert mock_load_via_dbt_ls.called
    assert mock_load_via_custom_parser.called


def test_load_manifest_without_manifest():
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / "jaffle_shop",
                manifest="/random/path/to/manifest.json",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                exclude=["config.materialized:table"],
                load_method=LoadMode.DBT_MANIFEST,
            ),
        ),
    )
    with pytest.raises(CosmosValueError) as err_info:
        dbt_graph.load()

    assert err_info.value.args[0] == "Unable to load manifest using /random/path/to/manifest.json"


@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
def test_load_manifest_with_manifest(mock_load_from_dbt_manifest):
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / "jaffle_shop",
                manifest=SAMPLE_MANIFEST,
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                exclude=["config.materialized:table"],
                load_method=LoadMode.DBT_MANIFEST,
            ),
        ),
    )
    dbt_graph.load()
    assert mock_load_from_dbt_manifest.called


@pytest.mark.parametrize(
    "exec_mode,method,expected_function",
    [
        (ExecutionMode.LOCAL, LoadMode.AUTOMATIC, "mock_load_via_dbt_ls"),
        (ExecutionMode.VIRTUALENV, LoadMode.AUTOMATIC, "mock_load_via_dbt_ls"),
        (ExecutionMode.KUBERNETES, LoadMode.AUTOMATIC, "mock_load_via_custom_parser"),
        (ExecutionMode.DOCKER, LoadMode.AUTOMATIC, "mock_load_via_custom_parser"),
        (ExecutionMode.LOCAL, LoadMode.DBT_LS, "mock_load_via_dbt_ls"),
        (ExecutionMode.LOCAL, LoadMode.CUSTOM, "mock_load_via_custom_parser"),
    ],
)
@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
def test_load(
    mock_load_from_dbt_manifest, mock_load_via_dbt_ls, mock_load_via_custom_parser, exec_mode, method, expected_function
):
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / "jaffle_shop",
                manifest="/random/path/to/manifest.json",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                exclude=["config.materialized:table"],
                load_method=method,
            ),
            execution_config=ExecutionConfig(
                execution_mode=exec_mode,
            ),
        ),
    )

    dbt_graph.load()
    load_function = locals()[expected_function]
    assert load_function.called


@pytest.mark.integration
def test_load_via_dbt_ls_with_exclude():
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / "jaffle_shop",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                select=["customers", "stg_customers", "raw_customers"],
            ),
        ),
    )
    dbt_graph.load_via_dbt_ls()
    expected_keys = [
        "model.jaffle_shop.customers",
        "model.jaffle_shop.stg_customers",
        "seed.jaffle_shop.raw_customers",
        "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
        "test.jaffle_shop.not_null_stg_customers_customer_id.e2cfb1f9aa",
        "test.jaffle_shop.relationships_orders_customer_id__customer_id__ref_customers_.c6ec7f58f2",
        "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1",
        "test.jaffle_shop.unique_stg_customers_customer_id.c7614daada",
    ]
    assert list(dbt_graph.nodes.keys()) == expected_keys
    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == 8

    sample_node = dbt_graph.nodes["model.jaffle_shop.customers"]
    assert sample_node.name == "customers"
    assert sample_node.unique_id == "model.jaffle_shop.customers"
    assert sample_node.resource_type == DbtResourceType.MODEL
    assert sample_node.depends_on == [
        "model.jaffle_shop.stg_customers",
        "model.jaffle_shop.stg_orders",
        "model.jaffle_shop.stg_payments",
    ]
    assert sample_node.file_path == DBT_PROJECTS_ROOT_DIR / "jaffle_shop/models/customers.sql"


@pytest.mark.integration
@pytest.mark.parametrize("pipeline_name", ("jaffle_shop", "jaffle_shop_python"))
def test_load_via_dbt_ls_without_exclude(pipeline_name):
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / pipeline_name,
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
        ),
    )
    dbt_graph.load_via_dbt_ls()

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == 28


def test_load_via_dbt_ls_with_invalid_dbt_path():
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / "jaffle_shop",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
            ),
            execution_config=ExecutionConfig(
                dbt_executable_path="/inexistent/dbt",
            ),
        ),
    )
    with pytest.raises(FileNotFoundError):
        dbt_graph.load_via_dbt_ls()


@pytest.mark.integration
@patch("cosmos.dbt.graph.Popen.communicate", return_value=("Some Runtime Error", ""))
def test_load_via_dbt_ls_with_runtime_error_in_stdout(mock_popen_communicate):
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / "jaffle_shop",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
            ),
        ),
    )
    # It may seem strange, but at least until dbt 1.6.0, there are circumstances when it outputs errors to stdout
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load_via_dbt_ls()
    expected = "Unable to run the command due to the error:\nSome Runtime Error"
    assert err_info.value.args[0] == expected
    mock_popen_communicate.assert_called_once()


@pytest.mark.parametrize("pipeline_name", ("jaffle_shop", "jaffle_shop_python"))
def test_load_via_load_via_custom_parser(pipeline_name):
    dbt_graph = DbtGraph(
        cosmos_config=CosmosConfig(
            project_config=ProjectConfig(
                dbt_project=DBT_PROJECTS_ROOT_DIR / pipeline_name,
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml=DBT_PROJECTS_ROOT_DIR / "jaffle_shop/profiles.yml",
            ),
            render_config=RenderConfig(
                load_method=LoadMode.CUSTOM,
            ),
        ),
    )
    dbt_graph.load_via_custom_parser()

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    # the custom parser does not add dbt test nodes
    assert len(dbt_graph.nodes) == 8
