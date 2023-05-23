from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.compat.functools import cached_property
from airflow.utils.context import Context
from airflow.utils.python_virtualenv import prepare_virtualenv

from cosmos.providers.dbt.core.operators.local import (
    DbtDocsLocalOperator,
    DbtLocalBaseOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtTestLocalOperator,
)

logger = logging.getLogger(__name__)


PY_INTERPRETER = "python3"


class DbtVirtualenvBaseOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core cli command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.

    :param install_deps: If true, install dependencies before running the command
    :param callback: A callback function called on after a dbt run with a path to the dbt project directory.
    :param py_requirements: Creates a virtual environment with the specified arguments
    :param py_system_site_packages: Whether or not all the Python packages from the Airflow instance, will be accessible
           within the virtual environment (if py_requirements argument is specified).
           Avoid using unless the DBT job requires it.
    """

    def __init__(
        self,
        py_requirements: list[str] | None = None,
        py_system_site_packages: bool = False,
        **kwargs,
    ) -> None:
        self.py_requirements = py_requirements
        self.py_system_site_packages = py_system_site_packages or []
        super().__init__(**kwargs)
        self._venv_tmp_dir = ""

    @cached_property
    def dbt_path(
        self,
    ) -> str:
        # We are reusing the virtualenv directory for all subprocess calls within this task/operator.
        # For this reason, we are not using contexts at this point.
        # The deletion of this virtualenv is being done by the end of the task execution
        self._venv_tmp_dir = TemporaryDirectory(prefix="cosmos-venv")
        py_interpreter = prepare_virtualenv(
            venv_directory=self._venv_tmp_dir.name,
            python_bin=PY_INTERPRETER,
            system_site_packages=self.py_system_site_packages,
            requirements=self.py_requirements,
        )
        dbt_binary = Path(py_interpreter).parent / "dbt"
        cmd_output = self.subprocess_hook.run_command(
            [
                py_interpreter,
                "-c",
                "from importlib.metadata import version; print(version('dbt-core'))",
            ]
        )
        dbt_version = cmd_output.output
        self.log.info("Using DBT version %s available at %s", dbt_version, dbt_binary)
        return str(dbt_binary)

    def run_subprocess(self, command, *args, **kwargs):
        if self.py_requirements:
            command[0] = self.dbt_path

        return self.subprocess_hook.run_command(
            command,
            *args,
            **kwargs,
        )

    def execute(self, context: Context) -> str:
        output = super().execute(context)
        self._venv_tmp_dir.cleanup()
        return output


class DbtLSVirtualenvOperator(DbtVirtualenvBaseOperator, DbtLSLocalOperator):
    """
    Executes a dbt core ls command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.
    """


class DbtSeedVirtualenvOperator(DbtVirtualenvBaseOperator, DbtSeedLocalOperator):
    """
    Executes a dbt core seed command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.
    """


class DbtSnapshotVirtualenvOperator(
    DbtVirtualenvBaseOperator, DbtSnapshotLocalOperator
):
    """
    Executes a dbt core snapshot command within a Python Virtual Environment, that is created before running the DBT
    command and deleted just after.
    """


class DbtRunVirtualenvOperator(DbtVirtualenvBaseOperator, DbtRunLocalOperator):
    """
    Executes a dbt core run command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.
    """


class DbtTestVirtualenvOperator(DbtVirtualenvBaseOperator, DbtTestLocalOperator):
    """
    Executes a dbt core test command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.
    """


class DbtRunOperationVirtualenvOperator(
    DbtVirtualenvBaseOperator, DbtRunOperationLocalOperator
):
    """
    Executes a dbt core run-operation command within a Python Virtual Environment, that is created before running the
    DBT command and deleted just after.
    """


class DbtDocsVirtualenvOperator(DbtVirtualenvBaseOperator, DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command within a Python Virtual Environment, that is created before running the DBT
    command and deleted just after.
    """
