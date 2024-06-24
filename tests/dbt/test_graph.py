import shutil
import tempfile
from pathlib import Path
from subprocess import PIPE, Popen
from unittest.mock import MagicMock, patch

import pytest
import yaml

from cosmos.config import CosmosConfigException, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DBT_TARGET_DIR_NAME, DbtResourceType, ExecutionMode
from cosmos.dbt.graph import (
    CosmosLoadDbtException,
    DbtGraph,
    DbtNode,
    LoadMode,
    parse_dbt_ls_output,
    run_command,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"
DBT_PROJECT_NAME = "jaffle_shop"
SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"
SAMPLE_MANIFEST_PY = Path(__file__).parent.parent / "sample/manifest_python.json"
SAMPLE_MANIFEST_MODEL_VERSION = Path(__file__).parent.parent / "sample/manifest_model_version.json"
SAMPLE_MANIFEST_SOURCE = Path(__file__).parent.parent / "sample/manifest_source.json"
SAMPLE_DBT_LS_OUTPUT = Path(__file__).parent.parent / "sample/sample_dbt_ls.txt"


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
    render_config = RenderConfig(exclude=["config.materialized:table"])
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
    render_config = RenderConfig(select=["+customers"])
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
    render_config = RenderConfig(dbt_ls_path=SAMPLE_DBT_LS_OUTPUT)
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
    render_config = RenderConfig(dbt_ls_path=None)
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
    render_config = RenderConfig(dbt_ls_path=SAMPLE_DBT_LS_OUTPUT, dbt_project_path=None)
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
        (ExecutionMode.VIRTUALENV, LoadMode.AUTOMATIC, "mock_load_via_custom_parser"),
        (ExecutionMode.KUBERNETES, LoadMode.AUTOMATIC, "mock_load_via_custom_parser"),
        (ExecutionMode.DOCKER, LoadMode.AUTOMATIC, "mock_load_via_custom_parser"),
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
@patch("cosmos.dbt.graph.Popen")
def test_load_via_dbt_ls_does_not_create_target_logs_in_original_folder(
    mock_popen, tmp_dbt_project_dir, postgres_profile_config
):
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    assert not (tmp_dbt_project_dir / "target").exists()
    assert not (tmp_dbt_project_dir / "logs").exists()

    project_config = ProjectConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    render_config = RenderConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
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

    used_cwd = Path(mock_popen.call_args[0][0][-5])
    assert used_cwd != project_config.dbt_project_path
    assert not used_cwd.exists()


@pytest.mark.integration
def test_load_via_dbt_ls_with_exclude(postgres_profile_config):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME, select=["*customers*"], exclude=["*orders*"]
    )
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )

    dbt_graph.load_via_dbt_ls()
    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    # This test is dependent upon dbt >= 1.5.4
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
    assert sorted(dbt_graph.nodes.keys()) == expected_keys

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
@pytest.mark.parametrize("project_name", ("jaffle_shop", "jaffle_shop_python"))
def test_load_via_dbt_ls_without_exclude(project_name, postgres_profile_config):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name)
    render_config = RenderConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )
    dbt_graph.load_via_dbt_ls()

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == 28


def test_load_via_custom_without_project_path():
    project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="test")
    execution_config = ExecutionConfig()
    render_config = RenderConfig(dbt_executable_path="/inexistent/dbt")
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
        dbt_executable_path="existing-dbt-cmd", dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME
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
        dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME, dbt_executable_path="/inexistent/dbt"
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


@pytest.mark.sqlite
@pytest.mark.parametrize("load_method", ["load_via_dbt_ls", "load_from_dbt_manifest"])
@pytest.mark.integration
def test_load_via_dbt_ls_with_sources(load_method):
    project_name = "simple"
    dbt_graph = DbtGraph(
        project=ProjectConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name,
            manifest_path=SAMPLE_MANIFEST_SOURCE if load_method == "load_from_dbt_manifest" else None,
        ),
        render_config=RenderConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name,
            dbt_deps=False,
            env_vars={"DBT_SQLITE_PATH": str(DBT_PROJECTS_ROOT_DIR / "data")},
        ),
        execution_config=ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name),
        profile_config=ProfileConfig(
            profile_name="simple",
            target_name="dev",
            profiles_yml_filepath=(DBT_PROJECTS_ROOT_DIR / project_name / "profiles.yml"),
        ),
    )
    getattr(dbt_graph, load_method)()
    assert len(dbt_graph.nodes) >= 4
    assert "source.simple.main.movies_ratings" in dbt_graph.nodes
    assert "exposure.simple.weekly_metrics" in dbt_graph.nodes


@pytest.mark.integration
def test_load_via_dbt_ls_without_dbt_deps(postgres_profile_config):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME, dbt_deps=False)
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )

    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load_via_dbt_ls()

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
    render_config = RenderConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME, dbt_deps=False)
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )

    assert dbt_graph.load_via_dbt_ls() is None  # Doesn't raise any exceptions


@pytest.mark.integration
def test_load_via_dbt_ls_caching_partial_parsing(tmp_dbt_project_dir, postgres_profile_config, caplog, tmp_path):
    """
    When using RenderConfig.enable_mock_profile=False and defining DbtGraph.cache_dir,
    Cosmos should leverage dbt partial parsing.
    """
    import logging

    caplog.set_level(logging.DEBUG)

    project_config = ProjectConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME, dbt_deps=True, enable_mock_profile=False
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
    dbt_graph.load_via_dbt_ls()
    assert "Unable to do partial parsing" in caplog.text

    # From the second time we run dbt ls onwards, we benefit from partial parsing
    caplog.clear()
    dbt_graph.load_via_dbt_ls()  # should not not raise exception
    assert not "Unable to do partial parsing" in caplog.text


@pytest.mark.integration
@patch("cosmos.dbt.graph.Popen")
def test_load_via_dbt_ls_with_zero_returncode_and_non_empty_stderr(
    mock_popen, tmp_dbt_project_dir, postgres_profile_config
):
    mock_popen().communicate.return_value = ("", "Some stderr warnings")
    mock_popen().returncode = 0

    project_config = ProjectConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    render_config = RenderConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
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
    render_config = RenderConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )
    expected = r"Unable to run \['.+dbt', 'deps', .*\] due to the error:\nSome stderr message"
    with pytest.raises(CosmosLoadDbtException, match=expected):
        dbt_graph.load_via_dbt_ls()


@pytest.mark.integration
@patch("cosmos.dbt.graph.Popen.communicate", return_value=("Some Runtime Error", ""))
def test_load_via_dbt_ls_with_runtime_error_in_stdout(mock_popen_communicate, postgres_profile_config):
    # It may seem strange, but at least until dbt 1.6.0, there are circumstances when it outputs errors to stdout
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )
    expected = r"Unable to run \['.+dbt', 'deps', .*\] due to the error:\nSome Runtime Error"
    with pytest.raises(CosmosLoadDbtException, match=expected):
        dbt_graph.load_via_dbt_ls()

    mock_popen_communicate.assert_called_once()


@pytest.mark.parametrize("project_name", ("jaffle_shop", "jaffle_shop_python"))
def test_load_via_load_via_custom_parser(project_name):
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name)
    execution_config = ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
    render_config = RenderConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME)
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

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == 28


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
    render_config = RenderConfig(exclude=["config.materialized:test"])
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
    render_config = RenderConfig(select=["tag:test_tag"])
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
        render_config=RenderConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / "model_version"),
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
        dbt_ls_path=SAMPLE_DBT_LS_OUTPUT, dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME
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


@patch("cosmos.dbt.graph.Popen")
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.config.RenderConfig.validate_dbt_command")
def test_load_via_dbt_ls_project_config_env_vars(mock_validate, mock_update_nodes, mock_popen, tmp_dbt_project_dir):
    """Tests that the dbt ls command in the subprocess has the project config env vars set."""
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    env_vars = {"MY_ENV_VAR": "my_value"}
    project_config = ProjectConfig(env_vars=env_vars)
    render_config = RenderConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
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


@patch("cosmos.dbt.graph.Popen")
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.config.RenderConfig.validate_dbt_command")
def test_load_via_dbt_ls_project_config_dbt_vars(mock_validate, mock_update_nodes, mock_popen, tmp_dbt_project_dir):
    """Tests that the dbt ls command in the subprocess has "--vars" with the project config dbt_vars."""
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    dbt_vars = {"my_var1": "my_value1", "my_var2": "my_value2"}
    project_config = ProjectConfig(dbt_vars=dbt_vars)
    render_config = RenderConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
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
    assert ls_command[ls_command.index("--vars") + 1] == yaml.dump(dbt_vars)


@patch("cosmos.dbt.graph.Popen")
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.config.RenderConfig.validate_dbt_command")
def test_load_via_dbt_ls_render_config_selector_arg_is_used(
    mock_validate, mock_update_nodes, mock_popen, tmp_dbt_project_dir
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


@patch("cosmos.dbt.graph.Popen")
@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency")
@patch("cosmos.config.RenderConfig.validate_dbt_command")
def test_load_via_dbt_ls_render_config_no_partial_parse(
    mock_validate, mock_update_nodes, mock_popen, tmp_dbt_project_dir
):
    """Tests that --no-partial-parse appears when partial_parse=False."""
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    project_config = ProjectConfig(partial_parse=False)
    render_config = RenderConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME, load_method=LoadMode.DBT_LS)
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
        render_config=RenderConfig(load_method=load_method, selector="my_selector"),
        project=MagicMock(),
    )
    with pytest.raises(CosmosLoadDbtException, match=expected_error_msg):
        dbt_graph.load(method=load_method)


@pytest.mark.sqlite
@pytest.mark.integration
def test_load_via_dbt_ls_with_project_config_vars():
    """
    Integration that tests that the dbt ls command is successful and that the node affected by the dbt_vars is
    rendered correctly.
    """
    project_name = "simple"
    dbt_graph = DbtGraph(
        project=ProjectConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name,
            env_vars={"DBT_SQLITE_PATH": str(DBT_PROJECTS_ROOT_DIR / "data")},
            dbt_vars={"animation_alias": "top_5_animated_movies"},
        ),
        render_config=RenderConfig(
            dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name,
            dbt_deps=False,
        ),
        execution_config=ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / project_name),
        profile_config=ProfileConfig(
            profile_name="simple",
            target_name="dev",
            profiles_yml_filepath=(DBT_PROJECTS_ROOT_DIR / project_name / "profiles.yml"),
        ),
    )
    dbt_graph.load_via_dbt_ls()
    assert dbt_graph.nodes["model.simple.top_animations"].config["alias"] == "top_5_animated_movies"


@pytest.mark.integration
def test_load_via_dbt_ls_with_selector_arg(tmp_dbt_project_dir, postgres_profile_config):
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
    with open(tmp_dbt_project_dir / DBT_PROJECT_NAME / "selectors.yml", "w") as f:
        f.write(selectors_yaml)

    project_config = ProjectConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    execution_config = ExecutionConfig(dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME)
    render_config = RenderConfig(
        dbt_project_path=tmp_dbt_project_dir / DBT_PROJECT_NAME,
        selector="stage_customers",
    )

    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=postgres_profile_config,
    )
    dbt_graph.load_via_dbt_ls()

    filtered_nodes = dbt_graph.filtered_nodes.keys()
    assert len(filtered_nodes) == 4
    assert "model.jaffle_shop.stg_customers" in filtered_nodes
    assert "seed.jaffle_shop.raw_customers" in filtered_nodes
    # Two tests should be filtered
    assert sum(node.startswith("test.jaffle_shop") for node in filtered_nodes) == 2
