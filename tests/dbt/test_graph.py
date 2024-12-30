import base64
import importlib
import json
import logging
import os
import shutil
import sys
import tempfile
import zlib
from datetime import datetime
from pathlib import Path
from subprocess import PIPE, Popen
from unittest.mock import MagicMock, patch

import pytest
from airflow.models import Variable

from cosmos import settings
from cosmos.config import CosmosConfigException, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DBT_TARGET_DIR_NAME, DbtResourceType, ExecutionMode, SourceRenderingBehavior
from cosmos.dbt.graph import (
    CosmosLoadDbtException,
    DbtGraph,
    DbtNode,
    LoadMode,
    parse_dbt_ls_output,
    run_command,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.settings import AIRFLOW_IO_AVAILABLE

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"
DBT_PROJECT_NAME = "jaffle_shop"
ALTERED_DBT_PROJECT_NAME = "altered_jaffle_shop"
SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"
SAMPLE_MANIFEST_PY = Path(__file__).parent.parent / "sample/manifest_python.json"
SAMPLE_MANIFEST_MODEL_VERSION = Path(__file__).parent.parent / "sample/manifest_model_version.json"
SAMPLE_MANIFEST_SOURCE = Path(__file__).parent.parent / "sample/manifest_source.json"
SAMPLE_DBT_LS_OUTPUT = Path(__file__).parent.parent / "sample/sample_dbt_ls.txt"
SOURCE_RENDERING_BEHAVIOR = SourceRenderingBehavior(os.getenv("SOURCE_RENDERING_BEHAVIOR", "none"))


@pytest.fixture
def tmp_dbt_project_dir():
    """
    Creates a plain dbt project structure, which does not contain logs or target folders.
    """
    source_proj_dir = DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME

    tmp_dir = Path(tempfile.mkdtemp())
    target_proj_dir = tmp_dir / DBT_PROJECT_NAME
    shutil.copytree(source_proj_dir, target_proj_dir)
    shutil.rmtree(target_proj_dir / "logs", ignore_errors=True)
    shutil.rmtree(target_proj_dir / "target", ignore_errors=True)
    yield tmp_dir

    shutil.rmtree(tmp_dir, ignore_errors=True)  # delete directory


@pytest.fixture
def tmp_altered_dbt_project_dir():
    """
    Creates a plain dbt project structure, which does not contain logs or target folders.
    """
    source_proj_dir = DBT_PROJECTS_ROOT_DIR / ALTERED_DBT_PROJECT_NAME

    tmp_dir = Path(tempfile.mkdtemp())
    target_proj_dir = tmp_dir / ALTERED_DBT_PROJECT_NAME
    shutil.copytree(source_proj_dir, target_proj_dir)
    shutil.rmtree(target_proj_dir / "logs", ignore_errors=True)
    shutil.rmtree(target_proj_dir / "target", ignore_errors=True)
    yield tmp_dir

    shutil.rmtree(tmp_dir, ignore_errors=True)  # delete directory


@pytest.fixture
def postgres_profile_config() -> ProfileConfig:
    return ProfileConfig(
        profile_name="default",
        target_name="default",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="example_conn",
            profile_args={"schema": "public"},
        ),
    )


@pytest.mark.parametrize(
    "unique_id,expected_name, expected_select",
    [
        ("model.my_project.customers", "customers", "customers"),
        ("model.my_project.customers.v1", "customers_v1", "customers.v1"),
        ("model.my_project.orders.v2", "orders_v2", "orders.v2"),
    ],
)
def test_dbt_node_name_and_select(unique_id, expected_name, expected_select):
    node = DbtNode(unique_id=unique_id, resource_type=DbtResourceType.MODEL, depends_on=[], file_path="")
    assert node.name == expected_name
    assert node.resource_name == expected_select


@pytest.mark.parametrize(
    "unique_id,expected_dict",
    [
        (
            "model.my_project.customers",
            {
                "unique_id": "model.my_project.customers",
                "resource_type": "model",
                "depends_on": [],
                "file_path": "",
                "tags": [],
                "config": {},
                "has_test": False,
                "resource_name": "customers",
                "name": "customers",
            },
        ),
        (
            "model.my_project.customers.v1",
            {
                "unique_id": "model.my_project.customers.v1",
                "resource_type": "model",
                "depends_on": [],
                "file_path": "",
                "tags": [],
                "config": {},
                "has_test": False,
                "resource_name": "customers.v1",
                "name": "customers_v1",
            },
        ),
    ],
)
def test_dbt_node_context_dict(
    unique_id,
    expected_dict,
):
    node = DbtNode(unique_id=unique_id, resource_type=DbtResourceType.MODEL, depends_on=[], file_path="")
    assert node.context_dict == expected_dict


@pytest.mark.parametrize(
    "project_name,manifest_filepath,model_filepath",
    [(DBT_PROJECT_NAME, SAMPLE_MANIFEST, "customers.sql"), ("jaffle_shop_python", SAMPLE_MANIFEST_PY, "customers.py")],
)
def test_load_via_manifest_with_exclude(project_name, manifest_filepath, model_filepath):
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name, manifest_path=manifest_filepath
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    render_config = RenderConfig(
        exclude=["config.materialized:table"],
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=project_config.dbt_project_path)
    dbt_graph = DbtGraph(
        project=project_config,
        execution_config=execution_config,
        profile_config=profile_config,
        render_config=render_config,
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
    assert sample_node.file_path == DBT_PROJECTS_ROOT_DIR / f"{project_name}/models/{model_filepath}"


@pytest.mark.parametrize(
    "project_name,manifest_filepath,model_filepath",
    [(DBT_PROJECT_NAME, SAMPLE_MANIFEST, "customers.sql"), ("jaffle_shop_python", SAMPLE_MANIFEST_PY, "customers.py")],
)
def test_load_via_manifest_with_select(project_name, manifest_filepath, model_filepath):
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name, manifest_path=manifest_filepath
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    render_config = RenderConfig(select=["+customers"], source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR)
    execution_config = ExecutionConfig(dbt_project_path=project_config.dbt_project_path)
    dbt_graph = DbtGraph(
        project=project_config,
        execution_config=execution_config,
        profile_config=profile_config,
        render_config=render_config,
    )
    dbt_graph.load_from_dbt_manifest()

    expected_keys = [
        "model.jaffle_shop.customers",
        "model.jaffle_shop.orders",
        "model.jaffle_shop.stg_customers",
        "model.jaffle_shop.stg_orders",
        "model.jaffle_shop.stg_payments",
        "seed.jaffle_shop.raw_customers",
        "seed.jaffle_shop.raw_orders",
        "seed.jaffle_shop.raw_payments",
        "test.jaffle_shop.accepted_values_orders_status__placed__shipped__completed__return_pending__returned.be6b5b5ec3",
        "test.jaffle_shop.accepted_values_stg_orders_status__placed__shipped__completed__return_pending__returned.080fb20aad",
        "test.jaffle_shop.accepted_values_stg_payments_payment_method__credit_card__coupon__bank_transfer__gift_card.3c3820f278",
        "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
        "test.jaffle_shop.not_null_orders_amount.106140f9fd",
        "test.jaffle_shop.not_null_orders_bank_transfer_amount.7743500c49",
        "test.jaffle_shop.not_null_orders_coupon_amount.ab90c90625",
        "test.jaffle_shop.not_null_orders_credit_card_amount.d3ca593b59",
        "test.jaffle_shop.not_null_orders_customer_id.c5f02694af",
        "test.jaffle_shop.not_null_orders_gift_card_amount.413a0d2d7a",
        "test.jaffle_shop.not_null_orders_order_id.cf6c17daed",
        "test.jaffle_shop.not_null_stg_customers_customer_id.e2cfb1f9aa",
        "test.jaffle_shop.not_null_stg_orders_order_id.81cfe2fe64",
        "test.jaffle_shop.not_null_stg_payments_payment_id.c19cc50075",
        "test.jaffle_shop.relationships_orders_customer_id__customer_id__ref_customers_.c6ec7f58f2",
        "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1",
        "test.jaffle_shop.unique_orders_order_id.fed79b3a6e",
        "test.jaffle_shop.unique_stg_customers_customer_id.c7614daada",
        "test.jaffle_shop.unique_stg_orders_order_id.e3b841c71a",
        "test.jaffle_shop.unique_stg_payments_payment_id.3744510712",
    ]
    assert sorted(dbt_graph.nodes.keys()) == expected_keys

    assert len(dbt_graph.nodes) == 28
    assert len(dbt_graph.filtered_nodes) == 7
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
    assert sample_node.file_path == DBT_PROJECTS_ROOT_DIR / f"{project_name}/models/{model_filepath}"


@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
def test_load_automatic_manifest_is_available(mock_load_from_dbt_manifest):
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME, manifest_path=SAMPLE_MANIFEST
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    dbt_graph = DbtGraph(project=project_config, profile_config=profile_config)
    dbt_graph.load(execution_mode=ExecutionMode.LOCAL)
    assert mock_load_from_dbt_manifest.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls_file", return_value=None)
def test_load_automatic_dbt_ls_file_is_available(mock_load_via_dbt_ls_file):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    render_config = RenderConfig(
        dbt_ls_path=SAMPLE_DBT_LS_OUTPUT,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    dbt_graph = DbtGraph(project=project_config, profile_config=profile_config, render_config=render_config)
    dbt_graph.load(method=LoadMode.DBT_LS_FILE, execution_mode=ExecutionMode.LOCAL)
    assert mock_load_via_dbt_ls_file.called


def test_load_dbt_ls_file_without_file():
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    render_config = RenderConfig(dbt_ls_path=None, source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR)
    dbt_graph = DbtGraph(project=project_config, profile_config=profile_config, render_config=render_config)
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load(execution_mode=ExecutionMode.LOCAL, method=LoadMode.DBT_LS_FILE)
    assert err_info.value.args[0] == "Unable to load dbt ls file using None"


def test_load_dbt_ls_file_without_project_path():
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    render_config = RenderConfig(
        dbt_ls_path=SAMPLE_DBT_LS_OUTPUT,
        dbt_project_path=None,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    dbt_graph = DbtGraph(
        project=project_config,
        profile_config=profile_config,
        render_config=render_config,
    )
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load(execution_mode=ExecutionMode.LOCAL, method=LoadMode.DBT_LS_FILE)
    assert err_info.value.args[0] == "Unable to load dbt ls file without RenderConfig.project_path"


@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", side_effect=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", return_value=None)
def test_load_automatic_without_manifest_with_profile_yml(mock_load_via_dbt_ls, mock_load_via_custom_parser):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    dbt_graph = DbtGraph(project=project_config, profile_config=profile_config)
    dbt_graph.load(execution_mode=ExecutionMode.LOCAL)
    assert mock_load_via_dbt_ls.called
    assert not mock_load_via_custom_parser.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", side_effect=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", return_value=None)
def test_load_automatic_without_manifest_with_profile_mapping(mock_load_via_dbt_ls, mock_load_via_custom_parser):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="example_conn",
            profile_args={"schema": "public"},
        ),
    )
    dbt_graph = DbtGraph(project=project_config, profile_config=profile_config)
    dbt_graph.load(execution_mode=ExecutionMode.LOCAL)
    assert mock_load_via_dbt_ls.called
    assert not mock_load_via_custom_parser.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", side_effect=FileNotFoundError())
def test_load_automatic_without_manifest_and_without_dbt_cmd(mock_load_via_dbt_ls, mock_load_via_custom_parser):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    dbt_graph = DbtGraph(project=project_config, profile_config=profile_config)
    dbt_graph.load(execution_mode=ExecutionMode.LOCAL, method=LoadMode.AUTOMATIC)
    assert mock_load_via_dbt_ls.called
    assert mock_load_via_custom_parser.called


def test_load_manifest_without_manifest():
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    dbt_graph = DbtGraph(project=project_config, profile_config=profile_config)
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load(execution_mode=ExecutionMode.LOCAL, method=LoadMode.DBT_MANIFEST)
    assert err_info.value.args[0] == "Unable to load manifest using None"


@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
def test_load_manifest_with_manifest(mock_load_from_dbt_manifest):
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME, manifest_path=SAMPLE_MANIFEST
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    dbt_graph = DbtGraph(project=project_config, profile_config=profile_config)
    dbt_graph.load(execution_mode=ExecutionMode.LOCAL, method=LoadMode.DBT_MANIFEST)
    assert mock_load_from_dbt_manifest.called


@pytest.mark.parametrize(
    "exec_mode,method,expected_function",
    [
        (ExecutionMode.LOCAL, LoadMode.AUTOMATIC, "mock_load_via_dbt_ls"),
        (ExecutionMode.VIRTUALENV, LoadMode.AUTOMATIC, "mock_load_via_dbt_ls"),
        (ExecutionMode.KUBERNETES, LoadMode.AUTOMATIC, "mock_load_via_dbt_ls"),
        (ExecutionMode.DOCKER, LoadMode.AUTOMATIC, "mock_load_via_dbt_ls"),
        (ExecutionMode.LOCAL, LoadMode.DBT_LS, "mock_load_via_dbt_ls"),
        (ExecutionMode.LOCAL, LoadMode.CUSTOM, "mock_load_via_custom_parser"),
    ],
)
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls_file", return_value=None)
def test_load(
    mock_load_from_dbt_manifest,
    mock_load_via_dbt_ls_file,
    mock_load_via_dbt_ls,
    mock_load_via_custom_parser,
    mock_update_node_dependency,
    exec_mode,
    method,
    expected_function,
):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    dbt_graph = DbtGraph(project=project_config, profile_config=profile_config)

    dbt_graph.load(method=method, execution_mode=exec_mode)
    load_function = locals()[expected_function]
    assert load_function.called
    assert mock_update_node_dependency.called


@pytest.mark.integration
@pytest.mark.parametrize("enable_cache_profile", [True, False])
@patch("cosmos.config.is_profile_cache_enabled")
@patch("cosmos.dbt.graph.Popen")
def test_load_via_dbt_ls_does_not_create_target_logs_in_original_folder(
    mock_popen, is_profile_cache_enabled, enable_cache_profile, tmp_dbt_project_dir, postgres_profile_config
):
    is_profile_cache_enabled.return_value = enable_cache_profile
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    assert not (tmp_dbt_project_dir / "target").exists()
    assert not (tmp_dbt_project_dir / "logs").exists()

    project_config = ProjectConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )
    dbt_graph.load_via_dbt_ls()
    assert not (tmp_dbt_project_dir / "target").exists()
    assert not (tmp_dbt_project_dir / "logs").exists()

    used_cwd = Path(mock_popen.call_args[0][0][5])
    assert used_cwd != project_config.dbt_project_path
    assert not used_cwd.exists()


@pytest.mark.integration
def test_load_via_dbt_ls_with_exclude(postgres_profile_config):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / ALTERED_DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / ALTERED_DBT_PROJECT_NAME,
        select=["*customers*"],
        exclude=["*orders*"],
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / ALTERED_DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )

    dbt_graph.load_via_dbt_ls()
    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    # This test is dependent upon dbt >= 1.5.4
    assert len(dbt_graph.nodes) == 9
    expected_keys = [
        "model.altered_jaffle_shop.customers",
        "model.altered_jaffle_shop.stg_customers",
        "seed.altered_jaffle_shop.raw_customers",
        "test.altered_jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
        "test.altered_jaffle_shop.not_null_stg_customers_customer_id.e2cfb1f9aa",
        "test.altered_jaffle_shop.source_not_null_postgres_db_raw_customers_id.de3e9fff76",
        "test.altered_jaffle_shop.source_unique_postgres_db_raw_customers_id.6e5ad1d707",
        "test.altered_jaffle_shop.unique_customers_customer_id.c5af1ff4b1",
        "test.altered_jaffle_shop.unique_stg_customers_customer_id.c7614daada",
    ]
    assert sorted(dbt_graph.nodes.keys()) == expected_keys

    sample_node = dbt_graph.nodes["model.altered_jaffle_shop.customers"]
    assert sample_node.name == "customers"
    assert sample_node.unique_id == "model.altered_jaffle_shop.customers"
    assert sample_node.resource_type == DbtResourceType.MODEL
    assert sample_node.depends_on == [
        "model.altered_jaffle_shop.stg_customers",
        "model.altered_jaffle_shop.stg_orders",
        "model.altered_jaffle_shop.stg_payments",
    ]
    assert sample_node.file_path == DBT_PROJECTS_ROOT_DIR / ALTERED_DBT_PROJECT_NAME / "models/customers.sql"


@pytest.mark.integration
@pytest.mark.parametrize(
    "project_dir,node_count",
    [(DBT_PROJECTS_ROOT_DIR / ALTERED_DBT_PROJECT_NAME, 39), (DBT_PROJECTS_ROOT_DIR / "jaffle_shop_python", 28)],
)
def test_load_via_dbt_ls_without_exclude(project_dir, node_count, postgres_profile_config):
    project_config = ProjectConfig(dbt_project_path=project_dir)
    render_config = RenderConfig(
        dbt_project_path=project_dir,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=project_dir)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )
    dbt_graph.load_via_dbt_ls()

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == node_count


def test_load_via_custom_without_project_path():
    project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="test")
    execution_config = ExecutionConfig()
    render_config = RenderConfig(
        dbt_executable_path="/inexistent/dbt",
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    dbt_graph = DbtGraph(
        project=project_config,
        execution_config=execution_config,
        render_config=render_config,
    )
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load_via_custom_parser()

    expected = "Unable to load dbt project without RenderConfig.dbt_project_path and ExecutionConfig.dbt_project_path"
    assert err_info.value.args[0] == expected


@patch("cosmos.config.RenderConfig.validate_dbt_command", return_value=None)
def test_load_via_dbt_ls_without_profile(mock_validate_dbt_command):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_executable_path="existing-dbt-cmd",
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    dbt_graph = DbtGraph(
        project=project_config,
        execution_config=execution_config,
        render_config=render_config,
    )
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load_via_dbt_ls()

    expected = "Unable to load project via dbt ls without a profile config."
    assert err_info.value.args[0] == expected


@patch("cosmos.dbt.executable.shutil.which", return_value=None)
def test_load_via_dbt_ls_with_invalid_dbt_path(mock_which):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME,
        dbt_executable_path="/inexistent/dbt",
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    with patch("pathlib.Path.exists", return_value=True):
        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="default",
                profiles_yml_filepath=Path(__file__).parent.parent / "sample/profiles.yml",
            ),
        )
        with pytest.raises(CosmosConfigException) as err_info:
            dbt_graph.load_via_dbt_ls()

    expected = "Unable to find the dbt executable, attempted: </inexistent/dbt> and <dbt>."
    assert err_info.value.args[0] == expected


@pytest.mark.parametrize("load_method", ["load_via_dbt_ls", "load_from_dbt_manifest"])
@pytest.mark.integration
def test_load_via_dbt_ls_with_sources(load_method):
    project_name = ALTERED_DBT_PROJECT_NAME
    dbt_graph = DbtGraph(
        project=ProjectConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name,
            manifest_path=SAMPLE_MANIFEST_SOURCE if load_method == "load_from_dbt_manifest" else None,
        ),
        render_config=RenderConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name,
            dbt_deps=True,
            source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
        ),
        execution_config=ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name),
        profile_config=ProfileConfig(
            profile_name="postgres_profile",
            target_name="dev",
            profiles_yml_filepath=(DBT_PROJECTS_ROOT_DIR / project_name / "profiles.yml"),
        ),
    )
    getattr(dbt_graph, load_method)()
    assert len(dbt_graph.nodes) >= 4
    assert "source.altered_jaffle_shop.postgres_db.raw_customers" in dbt_graph.nodes
    assert "exposure.altered_jaffle_shop.weekly_metrics" in dbt_graph.nodes


@pytest.mark.integration
def test_load_via_dbt_ls_without_dbt_deps(postgres_profile_config):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME,
        dbt_deps=False,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )

    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load_via_dbt_ls_without_cache()

    expected = "Unable to run dbt ls command due to missing dbt_packages. Set RenderConfig.dbt_deps=True."
    assert err_info.value.args[0] == expected


@pytest.mark.integration
def test_load_via_dbt_ls_without_dbt_deps_and_preinstalled_dbt_packages(
    tmp_dbt_project_dir, postgres_profile_config, caplog
):
    local_flags = [
        "--project-dir",
        tmp_dbt_project_dir / DBT_PROJECT_NAME,
        "--profiles-dir",
        tmp_dbt_project_dir / DBT_PROJECT_NAME,
        "--profile",
        "default",
        "--target",
        "dev",
    ]

    deps_command = ["dbt", "deps"]
    deps_command.extend(local_flags)
    process = Popen(
        deps_command,
        stdout=PIPE,
        stderr=PIPE,
        cwd=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        universal_newlines=True,
    )
    stdout, stderr = process.communicate()

    project_config = ProjectConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        dbt_deps=False,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )

    assert dbt_graph.load_via_dbt_ls() is None  # Doesn't raise any exceptions


@pytest.mark.integration
@pytest.mark.parametrize("enable_cache_profile", [True, False])
@patch("cosmos.config.is_profile_cache_enabled")
def test_load_via_dbt_ls_caching_partial_parsing(
    is_profile_cache_enabled, enable_cache_profile, tmp_dbt_project_dir, postgres_profile_config, caplog, tmp_path
):
    """
    When using RenderConfig.enable_mock_profile=False and defining DbtGraph.cache_dir,
    Cosmos should leverage dbt partial parsing.
    """
    caplog.set_level(logging.DEBUG)

    is_profile_cache_enabled.return_value = enable_cache_profile

    project_config = ProjectConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        dbt_deps=True,
        enable_mock_profile=False,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
        cache_dir=tmp_path,
    )

    (tmp_path / DBT_TARGET_DIR_NAME).mkdir(parents=True, exist_ok=True)

    # First time dbt ls is run, partial parsing was not cached, so we don't benefit from this
    dbt_graph.load_via_dbt_ls_without_cache()
    assert "Unable to do partial parsing" in caplog.text

    # From the second time we run dbt ls onwards, we benefit from partial parsing
    caplog.clear()
    dbt_graph.load_via_dbt_ls_without_cache()  # should not not raise exception
    assert not "Unable to do partial parsing" in caplog.text


@pytest.mark.integration
def test_load_via_dbt_ls_uses_partial_parse_when_cache_is_disabled(
    tmp_dbt_project_dir, postgres_profile_config, caplog, tmp_path
):
    """
    When using RenderConfig.enable_mock_profile=False and defining DbtGraph.cache_dir,
    Cosmos should leverage dbt partial parsing.
    """
    target_dir = tmp_dbt_project_dir / DBT_PROJECT_NAME / DBT_TARGET_DIR_NAME
    target_dir.mkdir(parents=True, exist_ok=True)

    partial_parse_cache_dir = Path("/tmp/cosmos")
    partial_parse_cache_dir.mkdir(parents=True, exist_ok=True)

    caplog.set_level(logging.DEBUG)
    project_config = ProjectConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        dbt_deps=True,
        enable_mock_profile=False,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
        cache_dir=partial_parse_cache_dir,
    )
    # Creates partial parse file with the expected version
    dbt_graph.load_via_dbt_ls_without_cache()  # should not not raise exception

    caplog.clear()

    # We copy a valid partial parse to the project directory
    shutil.copy(partial_parse_cache_dir / "target/partial_parse.msgpack", target_dir / "partial_parse.msgpack")

    # Run dbt ls without cache_dir, which disables cache:
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
        cache_dir="",  # Cache is disabled
    )
    # Should use the partial parse available in the original project folder
    dbt_graph.load_via_dbt_ls_without_cache()  # should not not raise exception

    assert not "Unable to do partial parsing" in caplog.text


@pytest.mark.integration
@patch("cosmos.dbt.graph.Popen")
def test_load_via_dbt_ls_with_zero_returncode_and_non_empty_stderr(
    mock_popen, tmp_dbt_project_dir, postgres_profile_config
):
    mock_popen().communicate.return_value = ("", "Some stderr warnings")
    mock_popen().returncode = 0

    project_config = ProjectConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )

    dbt_graph.load_via_dbt_ls()  # does not raise exception


@pytest.mark.integration
@patch("cosmos.dbt.graph.Popen")
def test_load_via_dbt_ls_with_non_zero_returncode(mock_popen, postgres_profile_config):
    mock_popen().communicate.return_value = ("", "Some stderr message")
    mock_popen().returncode = 1

    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )
    expected = r"Unable to run \['.+dbt', 'deps', .*\] due to the error:\nstderr: Some stderr message\nstdout: "
    with pytest.raises(CosmosLoadDbtException, match=expected):
        dbt_graph.load_via_dbt_ls()


@pytest.mark.integration
@patch("cosmos.dbt.graph.Popen.communicate", return_value=("Some Runtime Error", ""))
def test_load_via_dbt_ls_with_runtime_error_in_stdout(mock_popen_communicate, postgres_profile_config):
    # It may seem strange, but at least until dbt 1.6.0, there are circumstances when it outputs errors to stdout
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )
    expected = r"Unable to run \['.+dbt', 'deps', .*\] due to the error:\nstderr: \nstdout: Some Runtime Error"
    with pytest.raises(CosmosLoadDbtException, match=expected):
        dbt_graph.load_via_dbt_ls()

    mock_popen_communicate.assert_called_once()


@pytest.mark.parametrize("project_name,nodes_count", [("altered_jaffle_shop", 29), ("jaffle_shop_python", 28)])
def test_load_via_load_via_custom_parser(project_name, nodes_count):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name)
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name)
    render_config = RenderConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / project_name / "profiles.yml",
    )
    dbt_graph = DbtGraph(
        project=project_config,
        profile_config=profile_config,
        render_config=render_config,
        execution_config=execution_config,
    )

    dbt_graph.load_via_custom_parser()

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == nodes_count


def test_load_via_load_via_custom_parser_select_rendering_config():
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / "jaffle_shop")
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME,
        select=["customers"],
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    dbt_graph = DbtGraph(
        project=project_config,
        profile_config=profile_config,
        render_config=render_config,
        execution_config=execution_config,
    )

    dbt_graph.load_via_custom_parser()

    assert len(dbt_graph.filtered_nodes) == 1
    for model_name in dbt_graph.filtered_nodes:
        assert model_name == "customers"
        filter_node = dbt_graph.filtered_nodes.get(model_name)
        assert filter_node.file_path == DBT_PROJECTS_ROOT_DIR / "jaffle_shop/models/customers.sql"
        assert filter_node.tags == ["tags:customers"]


@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency", return_value=None)
def test_update_node_dependency_called(mock_update_node_dependency):
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME, manifest_path=SAMPLE_MANIFEST
    )
    execution_config = ExecutionConfig(dbt_project_path=project_config.dbt_project_path)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    dbt_graph = DbtGraph(project=project_config, execution_config=execution_config, profile_config=profile_config)
    dbt_graph.load()

    assert mock_update_node_dependency.called


def test_update_node_dependency_target_exist():
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME, manifest_path=SAMPLE_MANIFEST
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    execution_config = ExecutionConfig(dbt_project_path=project_config.dbt_project_path)
    dbt_graph = DbtGraph(project=project_config, execution_config=execution_config, profile_config=profile_config)
    dbt_graph.load()

    for _, nodes in dbt_graph.nodes.items():
        if nodes.resource_type == DbtResourceType.TEST:
            for node_id in nodes.depends_on:
                assert dbt_graph.nodes[node_id].has_test is True


def test_update_node_dependency_test_not_exist():
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME, manifest_path=SAMPLE_MANIFEST
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    render_config = RenderConfig(
        exclude=["config.materialized:test"],
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    execution_config = ExecutionConfig(dbt_project_path=project_config.dbt_project_path)
    dbt_graph = DbtGraph(
        project=project_config,
        execution_config=execution_config,
        profile_config=profile_config,
        render_config=render_config,
    )
    dbt_graph.load_from_dbt_manifest()

    for _, nodes in dbt_graph.filtered_nodes.items():
        assert nodes.has_test is False


def test_tag_selected_node_test_exist():
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME, manifest_path=SAMPLE_MANIFEST
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    render_config = RenderConfig(select=["tag:test_tag"], source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR)
    execution_config = ExecutionConfig(dbt_project_path=project_config.dbt_project_path)
    dbt_graph = DbtGraph(
        project=project_config,
        execution_config=execution_config,
        profile_config=profile_config,
        render_config=render_config,
    )
    dbt_graph.load()
    assert len(dbt_graph.filtered_nodes) > 0

    for _, node in dbt_graph.filtered_nodes.items():
        assert node.tags == ["test_tag"]
        if node.resource_type == DbtResourceType.MODEL:
            assert node.has_test is True


@pytest.mark.integration
@pytest.mark.parametrize("load_method", ["load_via_dbt_ls", "load_from_dbt_manifest"])
def test_load_dbt_ls_and_manifest_with_model_version(load_method, postgres_profile_config):
    dbt_graph = DbtGraph(
        project=ProjectConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / "model_version",
            manifest_path=SAMPLE_MANIFEST_MODEL_VERSION if load_method == "load_from_dbt_manifest" else None,
        ),
        render_config=RenderConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / "model_version",
            source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
        ),
        execution_config=ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / "model_version"),
        profile_config=postgres_profile_config,
    )
    getattr(dbt_graph, load_method)()
    expected_dbt_nodes = {
        "seed.jaffle_shop.raw_customers": "raw_customers",
        "seed.jaffle_shop.raw_orders": "raw_orders",
        "seed.jaffle_shop.raw_payments": "raw_payments",
        "model.jaffle_shop.stg_customers.v1": "stg_customers_v1",
        "model.jaffle_shop.stg_customers.v2": "stg_customers_v2",
        "model.jaffle_shop.customers.v1": "customers_v1",
        "model.jaffle_shop.customers.v2": "customers_v2",
        "model.jaffle_shop.stg_orders.v1": "stg_orders_v1",
        "model.jaffle_shop.stg_payments": "stg_payments",
        "model.jaffle_shop.orders": "orders",
    }

    for unique_id, name in expected_dbt_nodes.items():
        assert unique_id in dbt_graph.nodes
        assert name == dbt_graph.nodes[unique_id].name

    # Test dependencies
    assert {
        "model.jaffle_shop.stg_customers.v1",
        "model.jaffle_shop.stg_orders.v1",
        "model.jaffle_shop.stg_payments",
    } == set(dbt_graph.nodes["model.jaffle_shop.customers.v1"].depends_on)
    assert {
        "model.jaffle_shop.stg_customers.v2",
        "model.jaffle_shop.stg_orders.v1",
        "model.jaffle_shop.stg_payments",
    } == set(dbt_graph.nodes["model.jaffle_shop.customers.v2"].depends_on)
    assert {
        "model.jaffle_shop.stg_orders.v1",
        "model.jaffle_shop.stg_payments",
    } == set(dbt_graph.nodes["model.jaffle_shop.orders"].depends_on)


@pytest.mark.integration
def test_load_via_dbt_ls_file():
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    render_config = RenderConfig(
        dbt_ls_path=SAMPLE_DBT_LS_OUTPUT,
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    dbt_graph = DbtGraph(
        project=project_config,
        profile_config=profile_config,
        render_config=render_config,
    )
    dbt_graph.load(method=LoadMode.DBT_LS_FILE, execution_mode=ExecutionMode.LOCAL)

    expected_dbt_nodes = {
        "model.jaffle_shop.stg_customers": "stg_customers",
        "model.jaffle_shop.stg_orders": "stg_orders",
        "model.jaffle_shop.stg_payments": "stg_payments",
    }
    for unique_id, name in expected_dbt_nodes.items():
        assert unique_id in dbt_graph.nodes
        assert name == dbt_graph.nodes[unique_id].name
    # Test dependencies
    assert {"seed.jaffle_shop.raw_customers"} == set(dbt_graph.nodes["model.jaffle_shop.stg_customers"].depends_on)
    assert {"seed.jaffle_shop.raw_orders"} == set(dbt_graph.nodes["model.jaffle_shop.stg_orders"].depends_on)
    assert {"seed.jaffle_shop.raw_payments"} == set(dbt_graph.nodes["model.jaffle_shop.stg_payments"].depends_on)


@pytest.mark.parametrize(
    "stdout,returncode",
    [
        ("all good", None),
        ("WarnErrorOptions", None),
        pytest.param("fail", 599, marks=pytest.mark.xfail(raises=CosmosLoadDbtException)),
        pytest.param("Error", None, marks=pytest.mark.xfail(raises=CosmosLoadDbtException)),
    ],
)
@patch("cosmos.dbt.graph.Popen")
def test_run_command(mock_popen, stdout, returncode):
    fake_command = ["fake", "command"]
    fake_dir = Path("fake_dir")
    env_vars = {"fake": "env_var"}

    mock_popen.return_value.communicate.return_value = (stdout, "")
    mock_popen.return_value.returncode = returncode

    return_value = run_command(fake_command, fake_dir, env_vars)
    args, kwargs = mock_popen.call_args
    assert args[0] == fake_command
    assert kwargs["cwd"] == fake_dir
    assert kwargs["env"] == env_vars

    assert return_value == stdout


@patch("cosmos.dbt.graph.Popen")
def test_run_command_none_argument(mock_popen, caplog):
    fake_command = ["invalid-cmd", None]
    fake_dir = Path("fake_dir")
    env_vars = {"fake": "env_var"}

    mock_popen.return_value.communicate.return_value = ("Invalid None argument", None)
    with pytest.raises(CosmosLoadDbtException) as exc_info:
        run_command(fake_command, fake_dir, env_vars)

    expected = "Unable to run ['invalid-cmd', '<None>'] due to the error:\nstderr: None\nstdout: Invalid None argument"
    assert str(exc_info.value) == expected


def test_parse_dbt_ls_output_real_life_customer_bug(caplog):
    dbt_ls_output = """
11:20:43  Running with dbt=1.7.6
11:20:45  Registered adapter: bigquery=1.7.2
11:20:45  Unable to do partial parsing because saved manifest not found. Starting full parse.
/***************************/
Values returned by mac_get_values:
{}
/***************************/
{"name": "some_model", "resource_type": "model", "package_name": "some_package", "original_file_path": "models/some_model.sql", "unique_id": "model.some_package.some_model", "alias": "some_model_some_package_1_8_0", "config": {"enabled": true, "alias": "some_model_some_package-1.8.0", "schema": "some_schema", "database": null, "tags": [], "meta": {}, "group": null, "materialized": "view", "incremental_strategy": null, "persist_docs": {}, "post-hook": [], "pre-hook": [], "quoting": {}, "column_types": {}, "full_refresh": null, "unique_key": null, "on_schema_change": "ignore", "on_configuration_change": "apply", "grants": {}, "packages": [], "docs": {"show": true, "node_color": null}, "contract": {"enforced": false, "alias_types": true}, "access": "protected"}, "tags": [], "depends_on": {"macros": [], "nodes": ["source.some_source"]}}"""

    expected_nodes = {
        "model.some_package.some_model": DbtNode(
            unique_id="model.some_package.some_model",
            resource_type=DbtResourceType.MODEL,
            file_path=Path("fake-project/models/some_model.sql"),
            tags=[],
            config={
                "access": "protected",
                "alias": "some_model_some_package-1.8.0",
                "column_types": {},
                "contract": {
                    "alias_types": True,
                    "enforced": False,
                },
                "database": None,
                "docs": {
                    "node_color": None,
                    "show": True,
                },
                "enabled": True,
                "full_refresh": None,
                "grants": {},
                "group": None,
                "incremental_strategy": None,
                "materialized": "view",
                "meta": {},
                "on_configuration_change": "apply",
                "on_schema_change": "ignore",
                "packages": [],
                "persist_docs": {},
                "post-hook": [],
                "pre-hook": [],
                "quoting": {},
                "schema": "some_schema",
                "tags": [],
                "unique_key": None,
            },
            depends_on=["source.some_source"],
        ),
    }
    nodes = parse_dbt_ls_output(Path("fake-project"), dbt_ls_output)

    assert expected_nodes == nodes
    assert "Could not parse following the dbt ls line even though it was a valid JSON `{}" in caplog.text


def test_parse_dbt_ls_output():
    fake_ls_stdout = '{"resource_type": "model", "name": "fake-name", "original_file_path": "fake-file-path.sql", "unique_id": "fake-unique-id", "tags": [], "config": {}}'

    expected_nodes = {
        "fake-unique-id": DbtNode(
            unique_id="fake-unique-id",
            resource_type=DbtResourceType.MODEL,
            file_path=Path("fake-project/fake-file-path.sql"),
            tags=[],
            config={},
            depends_on=[],
        ),
    }
    nodes = parse_dbt_ls_output(Path("fake-project"), fake_ls_stdout)

    assert expected_nodes == nodes


def test_parse_dbt_ls_output_with_json_without_tags_or_config():
    some_ls_stdout = '{"resource_type": "model", "name": "some-name", "original_file_path": "some-file-path.sql", "unique_id": "some-unique-id", "config": {}}'

    expected_nodes = {
        "some-unique-id": DbtNode(
            unique_id="some-unique-id",
            resource_type=DbtResourceType.MODEL,
            file_path=Path("some-project/some-file-path.sql"),
            tags=[],
            config={},
            depends_on=[],
        ),
    }
    nodes = parse_dbt_ls_output(Path("some-project"), some_ls_stdout)

    assert expected_nodes == nodes


@patch("cosmos.dbt.graph.DbtGraph.should_use_dbt_ls_cache", return_value=False)
@patch("cosmos.dbt.graph.Popen")
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.config.RenderConfig.validate_dbt_command")
def test_load_via_dbt_ls_project_config_env_vars(
    mock_validate, mock_update_nodes, mock_popen, mock_enable_cache, tmp_dbt_project_dir
):
    """Tests that the dbt ls command in the subprocess has the project config env vars set."""
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    env_vars = {"MY_ENV_VAR": "my_value"}
    project_config = ProjectConfig(env_vars=env_vars)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=profile_config,
    )
    dbt_graph.load_via_dbt_ls()

    assert "MY_ENV_VAR" in mock_popen.call_args.kwargs["env"]
    assert mock_popen.call_args.kwargs["env"]["MY_ENV_VAR"] == "my_value"


@patch("cosmos.dbt.graph.DbtGraph.should_use_dbt_ls_cache", return_value=False)
@patch("cosmos.config.is_profile_cache_enabled", return_value=False)
@patch("cosmos.dbt.graph.Popen")
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.config.RenderConfig.validate_dbt_command")
def test_profile_created_correctly_with_profile_mapping(
    mock_validate,
    mock_update_nodes,
    mock_popen,
    mock_enable_profile_cache,
    mock_enable_cache,
    tmp_dbt_project_dir,
    postgres_profile_config,
):
    """Tests that the temporary profile is created without errors."""
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    project_config = ProjectConfig(env_vars={})
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    profile_config = postgres_profile_config
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=profile_config,
    )

    assert dbt_graph.load_via_dbt_ls() == None


@patch("cosmos.dbt.graph.DbtGraph.should_use_dbt_ls_cache", return_value=False)
@patch("cosmos.dbt.graph.Popen")
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.config.RenderConfig.validate_dbt_command")
def test_load_via_dbt_ls_project_config_dbt_vars(
    mock_validate, mock_update_nodes, mock_popen, mock_use_case, tmp_dbt_project_dir
):
    """Tests that the dbt ls command in the subprocess has "--vars" with the project config dbt_vars."""
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    dbt_vars = {"my_var1": "my_value1", "my_var2": "my_value2"}
    project_config = ProjectConfig(dbt_vars=dbt_vars)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=profile_config,
    )
    dbt_graph.load_via_dbt_ls()
    ls_command = mock_popen.call_args.args[0]
    assert "--vars" in ls_command
    assert ls_command[ls_command.index("--vars") + 1] == '{"my_var1": "my_value1", "my_var2": "my_value2"}'


@patch("cosmos.dbt.graph.DbtGraph.should_use_dbt_ls_cache", return_value=False)
@patch("cosmos.dbt.graph.Popen")
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.config.RenderConfig.validate_dbt_command")
def test_load_via_dbt_ls_render_config_selector_arg_is_used(
    mock_validate, mock_update_nodes, mock_popen, mock_enable_cache, tmp_dbt_project_dir
):
    """Tests that the dbt ls command in the subprocess has "--selector" with the RenderConfig.selector."""
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    selector = "my_selector"
    project_config = ProjectConfig()
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        load_method=LoadMode.DBT_LS,
        selector=selector,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    execution_config = MagicMock()
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=profile_config,
    )
    dbt_graph.load_via_dbt_ls()
    ls_command = mock_popen.call_args.args[0]
    assert "--selector" in ls_command
    assert ls_command[ls_command.index("--selector") + 1] == selector


@patch("cosmos.dbt.graph.DbtGraph.should_use_dbt_ls_cache", return_value=False)
@patch("cosmos.dbt.graph.Popen")
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.config.RenderConfig.validate_dbt_command")
def test_load_via_dbt_ls_render_config_no_partial_parse(
    mock_validate, mock_update_nodes, mock_popen, mock_enable_cache, tmp_dbt_project_dir
):
    """Tests that --no-partial-parse appears when partial_parse=False."""
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    project_config = ProjectConfig(partial_parse=False)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        load_method=LoadMode.DBT_LS,
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profiles_yml_filepath=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME / "profiles.yml",
    )
    execution_config = MagicMock()
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=profile_config,
    )
    dbt_graph.load_via_dbt_ls()
    ls_command = mock_popen.call_args.args[0]
    assert "--no-partial-parse" in ls_command


@pytest.mark.parametrize("load_method", [LoadMode.DBT_MANIFEST, LoadMode.CUSTOM])
def test_load_method_with_unsupported_render_config_selector_arg(load_method):
    """Tests that error is raised when RenderConfig.selector is used with LoadMode.DBT_MANIFEST or LoadMode.CUSTOM."""

    expected_error_msg = (
        f"RenderConfig.selector is not yet supported when loading dbt projects using the {load_method} parser."
    )
    dbt_graph = DbtGraph(
        render_config=RenderConfig(
            load_method=load_method,
            selector="my_selector",
            source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
        ),
        project=MagicMock(),
    )
    with pytest.raises(CosmosLoadDbtException, match=expected_error_msg):
        dbt_graph.load(method=load_method)


@pytest.mark.integration
def test_load_via_dbt_ls_with_project_config_vars():
    """
    Integration that tests that the dbt ls command is successful and that the node affected by the dbt_vars is
    rendered correctly.
    """
    project_name = ALTERED_DBT_PROJECT_NAME
    dbt_graph = DbtGraph(
        project=ProjectConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name,
            dbt_vars={"orders_alias": "orders"},
        ),
        render_config=RenderConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name,
            source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
        ),
        execution_config=ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name),
        profile_config=ProfileConfig(
            profile_name="postgres_profile",
            target_name="dev",
            profiles_yml_filepath=(DBT_PROJECTS_ROOT_DIR / project_name / "profiles.yml"),
        ),
    )
    dbt_graph.load_via_dbt_ls()
    assert dbt_graph.nodes["model.altered_jaffle_shop.orders"].config["alias"] == "orders"


@pytest.mark.integration
def test_load_via_dbt_ls_with_selector_arg(tmp_altered_dbt_project_dir, postgres_profile_config):
    """
    Tests that the dbt ls load method is successful if a selector arg is used with RenderConfig
    and that the filtered nodes are expected.
    """
    # Add a selectors yaml file to the project that will select the stg_customers model and all
    # parents (raw_customers)
    selectors_yaml = """
    selectors:
      - name: stage_customers
        definition:
          method: fqn
          value: stg_customers
          parents: true
    """
    with open(tmp_altered_dbt_project_dir / ALTERED_DBT_PROJECT_NAME / "selectors.yml", "w") as f:
        f.write(selectors_yaml)

    project_config = ProjectConfig(dbt_project_path=tmp_altered_dbt_project_dir / ALTERED_DBT_PROJECT_NAME)
    execution_config = ExecutionConfig(dbt_project_path=tmp_altered_dbt_project_dir / ALTERED_DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=tmp_altered_dbt_project_dir / ALTERED_DBT_PROJECT_NAME,
        selector="stage_customers",
        source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
    )

    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )
    dbt_graph.load_via_dbt_ls()

    filtered_nodes = dbt_graph.filtered_nodes.keys()
    assert len(filtered_nodes) == 7
    assert "model.altered_jaffle_shop.stg_customers" in filtered_nodes
    assert "seed.altered_jaffle_shop.raw_customers" in filtered_nodes
    if SOURCE_RENDERING_BEHAVIOR == SourceRenderingBehavior.ALL:
        assert "source.altered_jaffle_shop.postgres_db.raw_customers" in filtered_nodes
    # Four tests should be filtered
    assert sum(node.startswith("test.altered_jaffle_shop") for node in filtered_nodes) == 4


@pytest.mark.parametrize(
    "render_config,project_config,expected_envvars",
    [
        (RenderConfig(), ProjectConfig(), {}),
        (RenderConfig(env_vars={"a": 1}), ProjectConfig(), {"a": 1}),
        (RenderConfig(), ProjectConfig(env_vars={"b": 2}), {"b": 2}),
        (RenderConfig(env_vars={"a": 1}), ProjectConfig(env_vars={"b": 2}), {"a": 1}),
    ],
)
def test_env_vars(render_config, project_config, expected_envvars):
    graph = DbtGraph(
        project=project_config,
        render_config=render_config,
    )
    assert graph.env_vars == expected_envvars


def test_project_path_fails():
    graph = DbtGraph(project=ProjectConfig())
    with pytest.raises(CosmosLoadDbtException) as e:
        graph.project_path

    expected = "Unable to load project via dbt ls without RenderConfig.dbt_project_path, ProjectConfig.dbt_project_path or ExecutionConfig.dbt_project_path"
    assert e.value.args[0] == expected


@pytest.mark.parametrize(
    "render_config,project_config,expected_dbt_ls_args",
    [
        (RenderConfig(), ProjectConfig(), []),
        (RenderConfig(exclude=["package:snowplow"]), ProjectConfig(), ["--exclude", "package:snowplow"]),
        (
            RenderConfig(select=["tag:prod", "config.materialized:incremental"]),
            ProjectConfig(),
            ["--select", "tag:prod", "config.materialized:incremental"],
        ),
        (RenderConfig(selector="nightly"), ProjectConfig(), ["--selector", "nightly"]),
        (RenderConfig(), ProjectConfig(dbt_vars={"a": 1}), ["--vars", '{"a": 1}']),
        (RenderConfig(), ProjectConfig(partial_parse=False), ["--no-partial-parse"]),
        (
            RenderConfig(exclude=["1", "2"], select=["a", "b"], selector="nightly"),
            ProjectConfig(dbt_vars={"a": 1}, partial_parse=False),
            [
                "--exclude",
                "1",
                "2",
                "--select",
                "a",
                "b",
                "--vars",
                '{"a": 1}',
                "--selector",
                "nightly",
                "--no-partial-parse",
            ],
        ),
    ],
)
def test_dbt_ls_args(render_config, project_config, expected_dbt_ls_args):
    graph = DbtGraph(
        project=project_config,
        render_config=render_config,
    )
    assert graph.dbt_ls_args == expected_dbt_ls_args


def test_dbt_ls_cache_key_args_sorts_envvars():
    project_config = ProjectConfig(env_vars={11: "November", 12: "December", 5: "May"})
    graph = DbtGraph(project=project_config)
    assert graph.dbt_ls_cache_key_args == ['{"5": "May", "11": "November", "12": "December"}']


@patch("cosmos.dbt.graph.run_command")
def test_run_dbt_deps(run_command_mock):
    project_config = ProjectConfig(dbt_vars={"var-key": "var-value"})
    graph = DbtGraph(project=project_config)
    graph.local_flags = []
    graph.run_dbt_deps("dbt", "/some/path", {})
    run_command_mock.assert_called_with(["dbt", "deps", "--vars", '{"var-key": "var-value"}'], "/some/path", {})


@pytest.fixture()
def airflow_variable():
    key = "cosmos_cache__undefined"
    value = "some_value"
    Variable.set(key, value)

    yield key, value

    Variable.delete(key)


@pytest.mark.integration
def test_dbt_ls_cache_key_args_uses_airflow_vars_to_purge_dbt_ls_cache(airflow_variable):
    key, value = airflow_variable
    graph = DbtGraph(
        project=ProjectConfig(),
        render_config=RenderConfig(
            airflow_vars_to_purge_dbt_ls_cache=[key],
            source_rendering_behavior=SOURCE_RENDERING_BEHAVIOR,
        ),
    )
    assert graph.dbt_ls_cache_key_args == [key, value]


@patch("cosmos.dbt.graph.datetime")
@patch("cosmos.dbt.graph.Variable.set")
def test_save_dbt_ls_cache(mock_variable_set, mock_datetime, tmp_dbt_project_dir):
    mock_datetime.datetime.now.return_value = datetime(2022, 1, 1, 12, 0, 0)
    graph = DbtGraph(cache_identifier="something", project=ProjectConfig(dbt_project_path=tmp_dbt_project_dir))
    dbt_ls_output = "some output"
    graph.save_dbt_ls_cache(dbt_ls_output)
    assert mock_variable_set.call_args[0][0] == "cosmos_cache__something"
    assert mock_variable_set.call_args[0][1]["dbt_ls_compressed"] == "eJwrzs9NVcgvLSkoLQEAGpAEhg=="
    assert mock_variable_set.call_args[0][1]["last_modified"] == "2022-01-01T12:00:00"
    version = mock_variable_set.call_args[0][1].get("version")
    hash_dir, hash_args = version.split(",")
    assert hash_args == "d41d8cd98f00b204e9800998ecf8427e"
    if sys.platform == "darwin":
        assert hash_dir == "fa5edac64de49909d4b8cbc4dc8abd4f"
    else:
        assert hash_dir == "9c9f712b6f6f1ace880dfc7f5f4ff051"


@pytest.mark.integration
def test_get_dbt_ls_cache_returns_empty_if_non_json_var(airflow_variable):
    graph = DbtGraph(project=ProjectConfig())
    assert graph.get_dbt_ls_cache() == {}


@patch("cosmos.dbt.graph.Variable.get", return_value={"dbt_ls_compressed": "eJwrzs9NVcgvLSkoLQEAGpAEhg=="})
def test_get_dbt_ls_cache_returns_decoded_and_decompressed_value(mock_variable_get):
    graph = DbtGraph(project=ProjectConfig())
    assert graph.get_dbt_ls_cache() == {"dbt_ls": "some output"}


@patch("cosmos.dbt.graph.Variable.get", return_value={})
def test_get_dbt_ls_cache_returns_empty_dict_if_empty_dict_var(mock_variable_get):
    graph = DbtGraph(project=ProjectConfig())
    assert graph.get_dbt_ls_cache() == {}


@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls_without_cache")
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls_cache", return_value=True)
def test_load_via_dbt_ls_does_not_call_without_cache(mock_cache, mock_without_cache):
    graph = DbtGraph(project=ProjectConfig())
    graph.load_via_dbt_ls()
    assert mock_cache.called
    assert not mock_without_cache.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls_without_cache")
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls_cache", return_value=False)
def test_load_via_dbt_ls_calls_without_cache(mock_cache, mock_without_cache):
    graph = DbtGraph(project=ProjectConfig())
    graph.load_via_dbt_ls()
    assert mock_cache.called
    assert mock_without_cache.called


@patch("cosmos.dbt.graph.DbtGraph.should_use_dbt_ls_cache", return_value=False)
def test_load_via_dbt_ls_cache_is_false_if_disabled(mock_should_use_dbt_ls_cache):
    graph = DbtGraph(project=ProjectConfig())
    assert not graph.load_via_dbt_ls_cache()
    assert mock_should_use_dbt_ls_cache.called


@patch("cosmos.dbt.graph.DbtGraph.get_dbt_ls_cache", return_value={})
@patch("cosmos.dbt.graph.DbtGraph.should_use_dbt_ls_cache", return_value=True)
def test_load_via_dbt_ls_cache_is_false_if_no_cache(mock_should_use_dbt_ls_cache, mock_get_dbt_ls_cache):
    graph = DbtGraph(project=ProjectConfig(dbt_project_path="/tmp"))
    assert not graph.load_via_dbt_ls_cache()
    assert mock_should_use_dbt_ls_cache.called
    assert mock_get_dbt_ls_cache.called


@patch("cosmos.dbt.graph.cache._calculate_dbt_ls_cache_current_version", return_value=1)
@patch("cosmos.dbt.graph.DbtGraph.get_dbt_ls_cache", return_value={"version": 2, "dbt_ls": "output"})
@patch("cosmos.dbt.graph.DbtGraph.should_use_dbt_ls_cache", return_value=True)
def test_load_via_dbt_ls_cache_is_false_if_cache_is_outdated(
    mock_should_use_dbt_ls_cache, mock_get_dbt_ls_cache, mock_calculate_current_version
):
    graph = DbtGraph(project=ProjectConfig(dbt_project_path="/tmp"))
    assert not graph.load_via_dbt_ls_cache()
    assert mock_should_use_dbt_ls_cache.called
    assert mock_get_dbt_ls_cache.called
    assert mock_calculate_current_version.called


@patch("cosmos.dbt.graph.parse_dbt_ls_output", return_value={"some-node": {}})
@patch("cosmos.dbt.graph.cache._calculate_dbt_ls_cache_current_version", return_value=1)
@patch("cosmos.dbt.graph.DbtGraph.get_dbt_ls_cache", return_value={"version": 1, "dbt_ls": "output"})
@patch("cosmos.dbt.graph.DbtGraph.should_use_dbt_ls_cache", return_value=True)
def test_load_via_dbt_ls_cache_is_true(
    mock_should_use_dbt_ls_cache, mock_get_dbt_ls_cache, mock_calculate_current_version, mock_parse_dbt_ls_output
):
    graph = DbtGraph(project=ProjectConfig(dbt_project_path="/tmp"))
    assert graph.load_via_dbt_ls_cache()
    assert graph.load_method == LoadMode.DBT_LS_CACHE
    assert graph.nodes == {"some-node": {}}
    assert graph.filtered_nodes == {"some-node": {}}
    assert mock_should_use_dbt_ls_cache.called
    assert mock_get_dbt_ls_cache.called
    assert mock_calculate_current_version.called
    assert mock_parse_dbt_ls_output.called


@pytest.mark.parametrize(
    "enable_cache,enable_cache_dbt_ls,cache_id,should_use",
    [
        (False, True, "id", False),
        (True, False, "id", False),
        (False, False, "id", False),
        (True, True, "", False),
        (True, True, "id", True),
    ],
)
def test_should_use_dbt_ls_cache(enable_cache, enable_cache_dbt_ls, cache_id, should_use):
    with patch.dict(
        os.environ,
        {
            "AIRFLOW__COSMOS__ENABLE_CACHE": str(enable_cache),
            "AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS": str(enable_cache_dbt_ls),
        },
    ):
        importlib.reload(settings)
        graph = DbtGraph(cache_identifier=cache_id, project=ProjectConfig(dbt_project_path="/tmp"))
        graph.should_use_dbt_ls_cache.cache_clear()
        assert graph.should_use_dbt_ls_cache() == should_use


@pytest.mark.skipif(not AIRFLOW_IO_AVAILABLE, reason="Airflow did not have Object Storage until the 2.8 release")
@patch("airflow.io.path.ObjectStoragePath")
@patch("cosmos.config.ProjectConfig")
@patch("cosmos.dbt.graph._configure_remote_cache_dir")
def test_save_dbt_ls_cache_remote_cache_dir(
    mock_configure_remote_cache_dir, mock_project_config, mock_object_storage_path
):
    mock_remote_cache_dir_path = mock_object_storage_path.return_value
    mock_remote_cache_dir_path.exists.return_value = True

    mock_configure_remote_cache_dir.return_value = mock_remote_cache_dir_path

    dbt_ls_output = "sample dbt ls output"
    mock_project_config.dbt_vars = {"var1": "value1"}
    mock_project_config.env_vars = {"var1": "value1"}
    mock_project_config._calculate_dbt_ls_cache_current_version.return_value = "mock_version"
    dbt_graph = DbtGraph(project=mock_project_config)

    dbt_graph.save_dbt_ls_cache(dbt_ls_output)

    mock_remote_cache_key_path = mock_remote_cache_dir_path / dbt_graph.dbt_ls_cache_key / "dbt_ls_cache.json"
    mock_remote_cache_key_path.open.assert_called_once_with("w")


@pytest.mark.skipif(not AIRFLOW_IO_AVAILABLE, reason="Airflow did not have Object Storage until the 2.8 release")
@patch("airflow.io.path.ObjectStoragePath")
@patch("cosmos.config.ProjectConfig")
@patch("cosmos.dbt.graph._configure_remote_cache_dir")
def test_get_dbt_ls_cache_remote_cache_dir(
    mock_configure_remote_cache_dir, mock_project_config, mock_object_storage_path
):
    mock_remote_cache_dir_path = mock_object_storage_path.return_value
    mock_remote_cache_dir_path.exists.return_value = True
    mock_configure_remote_cache_dir.return_value = mock_remote_cache_dir_path

    dbt_ls_output = "sample dbt ls output"
    compressed_data = zlib.compress(dbt_ls_output.encode("utf-8"))
    encoded_data = base64.b64encode(compressed_data).decode("utf-8")

    cache_dict = {
        "version": "cache-version",
        "dbt_ls_compressed": encoded_data,
        "last_modified": "2024-08-13T12:34:56Z",
    }

    mock_remote_cache_key_path = mock_remote_cache_dir_path / "some_cache_key" / "dbt_ls_cache.json"
    mock_remote_cache_key_path.exists.return_value = True
    mock_remote_cache_key_path.open.return_value.__enter__.return_value.read.return_value = json.dumps(cache_dict)

    dbt_graph = DbtGraph(project=mock_project_config)

    result = dbt_graph.get_dbt_ls_cache()

    expected_result = {
        "version": "cache-version",
        "dbt_ls": dbt_ls_output,
        "last_modified": "2024-08-13T12:34:56Z",
    }

    assert result == expected_result
