import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos.config import ProfileConfig
from cosmos.constants import DbtResourceType, ExecutionMode
from cosmos.dbt.graph import CosmosLoadDbtException, DbtGraph, LoadMode
from cosmos.dbt.project import DbtProject
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"
DBT_PIPELINE_NAME = "jaffle_shop"
SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"
SAMPLE_MANIFEST_PY = Path(__file__).parent.parent / "sample/manifest_python.json"
SAMPLE_MANIFEST_MODEL_VERSION = Path(__file__).parent.parent / "sample/manifest_model_version.json"


@pytest.fixture
def tmp_dbt_project_dir():
    """
    Creates a plain dbt project structure, which does not contain logs or target folders.
    """
    source_proj_dir = DBT_PROJECTS_ROOT_DIR / DBT_PIPELINE_NAME

    tmp_dir = Path(tempfile.mkdtemp())
    target_proj_dir = tmp_dir / DBT_PIPELINE_NAME
    shutil.copytree(source_proj_dir, target_proj_dir)
    shutil.rmtree(target_proj_dir / "logs", ignore_errors=True)
    shutil.rmtree(target_proj_dir / "target", ignore_errors=True)
    yield tmp_dir

    shutil.rmtree(tmp_dir, ignore_errors=True)  # delete directory


@pytest.mark.parametrize(
    "pipeline_name,manifest_filepath,model_filepath",
    [("jaffle_shop", SAMPLE_MANIFEST, "customers.sql"), ("jaffle_shop_python", SAMPLE_MANIFEST_PY, "customers.py")],
)
def test_load_via_manifest_with_exclude(pipeline_name, manifest_filepath, model_filepath):
    dbt_project = DbtProject(name=pipeline_name, root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path=manifest_filepath)
    dbt_graph = DbtGraph(project=dbt_project, exclude=["config.materialized:table"])
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
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path=SAMPLE_MANIFEST)
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load(execution_mode=ExecutionMode.LOCAL)
    assert mock_load_from_dbt_manifest.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", side_effect=FileNotFoundError())
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", return_value=None)
def test_load_automatic_without_manifest(mock_load_via_dbt_ls, mock_load_via_custom_parser):
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path="/tmp/manifest.json")
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load(execution_mode=ExecutionMode.LOCAL)
    assert mock_load_via_dbt_ls.called
    assert not mock_load_via_custom_parser.called


@patch("cosmos.dbt.graph.DbtGraph.load_via_custom_parser", return_value=None)
@patch("cosmos.dbt.graph.DbtGraph.load_via_dbt_ls", side_effect=FileNotFoundError())
def test_load_automatic_without_manifest_and_without_dbt_cmd(mock_load_via_dbt_ls, mock_load_via_custom_parser):
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load(execution_mode=ExecutionMode.LOCAL, method=LoadMode.AUTOMATIC)
    assert mock_load_via_dbt_ls.called
    assert mock_load_via_custom_parser.called


def test_load_manifest_without_manifest():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(project=dbt_project)
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load(execution_mode=ExecutionMode.LOCAL, method=LoadMode.DBT_MANIFEST)
    assert err_info.value.args[0] == "Unable to load manifest using None"


@patch("cosmos.dbt.graph.DbtGraph.load_from_dbt_manifest", return_value=None)
def test_load_manifest_with_manifest(mock_load_from_dbt_manifest):
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path=SAMPLE_MANIFEST)
    dbt_graph = DbtGraph(project=dbt_project)
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
@patch("cosmos.dbt.graph.Popen")
def test_load_via_dbt_ls_does_not_create_target_logs_in_original_folder(mock_popen, tmp_dbt_project_dir):
    mock_popen().communicate.return_value = ("", "")
    mock_popen().returncode = 0
    assert not (tmp_dbt_project_dir / "target").exists()
    assert not (tmp_dbt_project_dir / "logs").exists()

    dbt_project = DbtProject(name=DBT_PIPELINE_NAME, root_dir=tmp_dbt_project_dir)
    dbt_graph = DbtGraph(
        project=dbt_project,
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="default",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="airflow_db",
                profile_args={"schema": "public"},
            ),
        ),
    )
    dbt_graph.load_via_dbt_ls()
    assert not (tmp_dbt_project_dir / "target").exists()
    assert not (tmp_dbt_project_dir / "logs").exists()

    used_cwd = Path(mock_popen.call_args[0][0][-5])
    assert used_cwd != dbt_project.dir
    assert not used_cwd.exists()


@pytest.mark.integration
def test_load_via_dbt_ls_with_exclude():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(
        project=dbt_project,
        select=["*customers*"],
        exclude=["*orders*"],
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="default",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="airflow_db",
                profile_args={"schema": "public"},
            ),
        ),
    )

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
    dbt_project = DbtProject(name=pipeline_name, root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(
        project=dbt_project,
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="default",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="airflow_db",
                profile_args={"schema": "public"},
            ),
        ),
    )
    dbt_graph.load_via_dbt_ls()

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == 28


def test_load_via_dbt_ls_without_profile():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(
        dbt_cmd="/inexistent/dbt",
        project=dbt_project,
    )
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load_via_dbt_ls()

    expected = "Unable to load dbt project without a profile config"
    assert err_info.value.args[0] == expected


def test_load_via_dbt_ls_with_invalid_dbt_path():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    with patch("pathlib.Path.exists", return_value=True):
        dbt_graph = DbtGraph(
            dbt_cmd="/inexistent/dbt",
            project=dbt_project,
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="default",
                profiles_yml_filepath=Path(__file__).parent.parent / "sample/profiles.yml",
            ),
        )
        with pytest.raises(CosmosLoadDbtException) as err_info:
            dbt_graph.load_via_dbt_ls()

    expected = "Unable to find the dbt executable: /inexistent/dbt"
    assert err_info.value.args[0] == expected


@pytest.mark.integration
def test_load_via_dbt_ls_without_dbt_deps():
    pipeline_name = "jaffle_shop"
    dbt_project = DbtProject(name=pipeline_name, root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(
        dbt_deps=False,
        project=dbt_project,
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="default",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="airflow_db",
                profile_args={"schema": "public"},
            ),
        ),
    )

    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load_via_dbt_ls()

    expected = "Unable to run dbt ls command due to missing dbt_packages. Set render_config.dbt_deps=True."
    assert err_info.value.args[0] == expected


@pytest.mark.integration
@patch("cosmos.dbt.graph.Popen")
def test_load_via_dbt_ls_with_zero_returncode_and_non_empty_stderr(mock_popen, tmp_dbt_project_dir):
    mock_popen().communicate.return_value = ("", "Some stderr warnings")
    mock_popen().returncode = 0

    dbt_project = DbtProject(name=DBT_PIPELINE_NAME, root_dir=tmp_dbt_project_dir)
    dbt_graph = DbtGraph(
        project=dbt_project,
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="default",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="airflow_db",
                profile_args={"schema": "public"},
            ),
        ),
    )

    dbt_graph.load_via_dbt_ls()  # does not raise exception


@pytest.mark.integration
@patch("cosmos.dbt.graph.Popen")
def test_load_via_dbt_ls_with_non_zero_returncode(mock_popen):
    mock_popen().communicate.return_value = ("", "Some stderr message")
    mock_popen().returncode = 1

    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(
        project=dbt_project,
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="default",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="airflow_db",
                profile_args={"schema": "public"},
            ),
        ),
    )
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load_via_dbt_ls()

    expected = "Unable to run dbt deps command due to the error:\nSome stderr message"
    assert err_info.value.args[0] == expected


@pytest.mark.integration
@patch("cosmos.dbt.graph.Popen.communicate", return_value=("Some Runtime Error", ""))
def test_load_via_dbt_ls_with_runtime_error_in_stdout(mock_popen_communicate):
    # It may seem strange, but at least until dbt 1.6.0, there are circumstances when it outputs errors to stdout
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR)
    dbt_graph = DbtGraph(
        project=dbt_project,
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="default",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="airflow_db",
                profile_args={"schema": "public"},
            ),
        ),
    )
    with pytest.raises(CosmosLoadDbtException) as err_info:
        dbt_graph.load_via_dbt_ls()

    expected = "Unable to run dbt deps command due to the error:\nSome Runtime Error"
    assert err_info.value.args[0] == expected
    mock_popen_communicate.assert_called_once()


@pytest.mark.parametrize("pipeline_name", ("jaffle_shop", "jaffle_shop_python"))
def test_load_via_load_via_custom_parser(pipeline_name):
    dbt_project = DbtProject(
        name=pipeline_name,
        root_dir=DBT_PROJECTS_ROOT_DIR,
    )
    dbt_graph = DbtGraph(project=dbt_project)

    dbt_graph.load_via_custom_parser()

    assert dbt_graph.nodes == dbt_graph.filtered_nodes
    assert len(dbt_graph.nodes) == 28


@patch("cosmos.dbt.graph.DbtGraph.update_node_dependency", return_value=None)
def test_update_node_dependency_called(mock_update_node_dependency):
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path=SAMPLE_MANIFEST)
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load()

    assert mock_update_node_dependency.called


def test_update_node_dependency_target_exist():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path=SAMPLE_MANIFEST)
    dbt_graph = DbtGraph(project=dbt_project)
    dbt_graph.load()

    for _, nodes in dbt_graph.nodes.items():
        if nodes.resource_type == DbtResourceType.TEST:
            for node_id in nodes.depends_on:
                assert dbt_graph.nodes[node_id].has_test is True


def test_update_node_dependency_test_not_exist():
    dbt_project = DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR, manifest_path=SAMPLE_MANIFEST)
    dbt_graph = DbtGraph(project=dbt_project, exclude=["config.materialized:test"])
    dbt_graph.load_from_dbt_manifest()

    for _, nodes in dbt_graph.filtered_nodes.items():
        assert nodes.has_test is False


@pytest.mark.integration
@pytest.mark.parametrize("load_method", ["load_via_dbt_ls", "load_from_dbt_manifest"])
def test_load_dbt_ls_and_manifest_with_model_version(load_method):
    dbt_graph = DbtGraph(
        project=DbtProject(
            name="model_version",
            root_dir=DBT_PROJECTS_ROOT_DIR,
            manifest_path=SAMPLE_MANIFEST_MODEL_VERSION if load_method == "load_from_dbt_manifest" else None,
        ),
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="default",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="airflow_db",
                profile_args={"schema": "public"},
            ),
        ),
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
