import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

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
    open(str(file_to_be_changed), "w").close()

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
    # THe following command will fail because we didn't run `dbt deps` in advance
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
    # THe following command will fail because we didn't run `dbt deps` in advance
    response = dbt_runner.run_command(command=["dbt", "run"], env=os.environ, cwd=invalid_dbt_project_dir)
    assert not response.success
    assert response.exception is None
    assert response.result

    with pytest.raises(CosmosDbtRunError) as exc_info:
        dbt_runner.handle_exception_if_needed(response)

    err_msg = str(exc_info.value)
    expected1 = "dbt invocation completed with errors:"
    expected2 = "stg_payments: Database Error in model stg_payments"
    assert expected1 in err_msg
    assert expected2 in err_msg
