import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from airflow import DAG
from pendulum import datetime

from cosmos.config import InvocationMode, ProfileConfig
from cosmos.operators.local import DbtRunLocalOperator
from cosmos.operators.watcher import DbtProducerWatcherOperator

sys.modules.pop("dbt.cli.main", None)

import pytest

import cosmos.dbt.runner as dbt_runner
from cosmos.exceptions import CosmosDbtRunError

DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"


@pytest.fixture
def valid_dbt_project_dir():
    """
    Creates a plain dbt project structure, which does not contain logs or target folders.
    """
    tmp_dir = Path(tempfile.mkdtemp())
    source_proj_dir = DBT_PROJECT_PATH
    target_proj_dir = tmp_dir / "jaffle_shop"
    shutil.copytree(source_proj_dir, target_proj_dir)
    shutil.rmtree(target_proj_dir / "logs", ignore_errors=True)
    shutil.rmtree(target_proj_dir / "target", ignore_errors=True)
    yield target_proj_dir

    shutil.rmtree(tmp_dir, ignore_errors=True)  # delete directory


@pytest.fixture
def invalid_dbt_project_dir(valid_dbt_project_dir):
    """
    Create an invalid dbt project dir, that will raise exceptions if attempted to be run.
    """
    file_to_be_deleted = valid_dbt_project_dir / "packages.yml"
    file_to_be_deleted.unlink()

    file_to_be_changed = valid_dbt_project_dir / "models/staging/stg_orders.sql"
    with open(str(file_to_be_changed), "w") as fp:
        fp.writelines("select 1 as id")

    return valid_dbt_project_dir


@patch.dict(sys.modules, {"dbt.cli.main": None})
def test_is_available_is_false():
    assert not dbt_runner.is_available()


@pytest.mark.integration
def test_is_available_is_true():
    assert dbt_runner.is_available()


@pytest.mark.integration
def test_get_runner():
    from dbt.cli.main import dbtRunner

    runner = dbt_runner.get_runner()
    assert isinstance(runner, dbtRunner)


@pytest.mark.integration
def test_run_command(valid_dbt_project_dir):
    from dbt.cli.main import dbtRunnerResult

    response = dbt_runner.run_command(command=["dbt", "deps"], env=os.environ, cwd=valid_dbt_project_dir)
    assert isinstance(response, dbtRunnerResult)
    assert response.success
    assert response.exception is None
    assert response.result is None

    assert dbt_runner.handle_exception_if_needed(response) is None


@pytest.mark.integration
def test_handle_exception_if_needed_after_exception(valid_dbt_project_dir):
    # The following command will fail because we didn't run `dbt deps` in advance
    response = dbt_runner.run_command(command=["dbt", "ls"], env=os.environ, cwd=valid_dbt_project_dir)
    assert not response.success
    assert response.exception

    with pytest.raises(CosmosDbtRunError) as exc_info:
        dbt_runner.handle_exception_if_needed(response)

    err_msg = str(exc_info.value)
    expected1 = "dbt invocation did not complete with unhandled error: Compilation Error"
    expected2 = "dbt found 1 package(s) specified in packages.yml, but only 0 package(s) installed"
    assert expected1 in err_msg
    assert expected2 in err_msg


@pytest.mark.integration
def test_handle_exception_if_needed_after_error(invalid_dbt_project_dir):
    # The following command fails, but has no exceptions - only results
    response = dbt_runner.run_command(command=["dbt", "run"], env=os.environ, cwd=invalid_dbt_project_dir)
    assert not response.success
    assert response.exception is None
    assert response.result

    with pytest.raises(CosmosDbtRunError) as exc_info:
        dbt_runner.handle_exception_if_needed(response)

    err_msg = str(exc_info.value)
    expected1 = "dbt invocation completed with errors:"
    assert expected1 in err_msg


@pytest.mark.integration
def test_dbt_runner_caching_and_callbacks(valid_dbt_project_dir):
    """Test that:
    1. DbtRunLocalOperator uses cached runner (no callbacks)
    2. DbtProducerWatcherOperator creates new runner with callbacks
    """
    # Track dbtRunner instances
    instances = []

    class _MockTI:
        """Mock TaskInstance with required attributes."""

        def __init__(self):
            self.openlineage_events_completes = []
            self.store = {}

        def xcom_push(self, key, value, **_):
            self.store[key] = value

    class _FakeResult:
        """Mock dbtRunnerResult."""

        def __init__(self):
            self.success = True
            self.result = None

    class _FakeRunner:
        """Mock dbtRunner that tracks instances."""

        def __init__(self, callbacks=None):
            self.callbacks = callbacks or []
            instances.append(self)

        def invoke(self, *args):
            return _FakeResult()

    # Create mock context with task_instance
    mock_ti = _MockTI()
    mock_context = {
        "ti": mock_ti,
        "task_instance": mock_ti,
        "run_id": "test_run",
    }

    mock_profile = ProfileConfig(
        profile_name="test", target_name="test", profiles_yml_filepath=str(valid_dbt_project_dir / "profiles.yml")
    )

    with DAG(
        "test_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
    ) as dag:
        with patch.dict(
            sys.modules,
            {
                "dbt": type("dbt", (), {}),
                "dbt.cli": type("dbt.cli", (), {}),
                "dbt.cli.main": type("dbt.cli.main", (), {"dbtRunner": _FakeRunner}),
                "dbt.version": type("dbt.version", (), {"__version__": "1.9.0"}),
            },
        ), patch(
            "cosmos.operators.local.DbtLocalBaseOperator.build_cmd",
            return_value=(["dbt", "run"], {}),
        ), patch(
            "cosmos.operators.local.AbstractDbtLocalBase._handle_post_execution"
        ):
            # First operator - DbtRunLocalOperator should use cached runner
            op1 = DbtRunLocalOperator(
                task_id="dbt_run",
                project_dir=str(valid_dbt_project_dir),
                profile_config=mock_profile,
                install_deps=False,
            )
            mock_context["dag"] = dag
            op1.execute(context=mock_context)

            # Second operator - DbtProducerWatcherOperator should create new runner with callback
            op2 = DbtProducerWatcherOperator(
                task_id="dbt_watch",
                project_dir=str(valid_dbt_project_dir),
                profile_config=mock_profile,
                install_deps=False,
            )
            op2.invocation_mode = InvocationMode.DBT_RUNNER

            class _DummyEv:
                pass

            with patch("cosmos.operators.watcher.EventMsg", _DummyEv):
                op2.execute(context=mock_context)

            # Verify:
            # 1. We have two dbt Runner instances (cached + new with callbacks)
            assert len(instances) == 2
            # 2. First instance (cached) has no callbacks
            assert not instances[0].callbacks
            # 3. Second instance has one callback
            assert len(instances[1].callbacks) == 1
