from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from airflow.compat.functools import cached_property
from airflow.utils.python_virtualenv import prepare_virtualenv
from cosmos.hooks.subprocess import FullOutputSubprocessResult

from cosmos.log import get_logger
from cosmos.operators.local import (
    DbtDocsLocalOperator,
    DbtLocalBaseOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtTestLocalOperator,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = get_logger(__name__)


PY_INTERPRETER = "python3"


class DbtVirtualenvBaseOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core cli command within a Python Virtual Environment, that is created before running the dbt command
    and deleted at the end of the operator execution.

    :param py_requirements: If defined, creates a virtual environment with the specified dependencies. Example:
           ["dbt-postgres==1.5.0"]
    :param py_system_site_packages: Whether or not all the Python packages from the Airflow instance will be accessible
           within the virtual environment (if py_requirements argument is specified).
           Avoid using unless the dbt job requires it.
    """

    def __init__(
        self,
        py_requirements: list[str] | None = None,
        py_system_site_packages: bool = False,
        virtualenv_dir: Path | None = None,
        **kwargs: Any,
    ) -> None:
        self.py_requirements = py_requirements or []
        self.py_system_site_packages = py_system_site_packages
        super().__init__(**kwargs)
        self._venv_dir = virtualenv_dir
        self._venv_tmp_dir: None | TemporaryDirectory[str] = None

    @cached_property
    def venv_dbt_path(
        self,
    ) -> str:
        """
        Path to the dbt binary within a Python virtualenv.

        The first time this property is called, it creates a new/temporary and installs the dependencies
        based on the self.py_requirements and self.py_system_site_packages,  or retrieves an existing virtualenv.
        This value is cached for future calls.
        """
        # We are reusing the virtualenv directory for all subprocess calls within this task/operator.
        # For this reason, we are not using contexts at this point.
        # The deletion of this directory is done explicitly at the end of the `execute` method.
        py_interpreter = self._get_or_create_venv_py_interpreter()
        dbt_binary = Path(py_interpreter).parent / "dbt"
        cmd_output = self.subprocess_hook.run_command(
            [
                py_interpreter,
                "-c",
                "from importlib.metadata import version; print(version('dbt-core'))",
            ]
        )
        dbt_version = cmd_output.output
        self.log.info("Using dbt version %s available at %s", dbt_version, dbt_binary)
        return str(dbt_binary)

    def run_subprocess(self, *args: Any, command: list[str], **kwargs: Any) -> FullOutputSubprocessResult:
        if self.py_requirements:
            command[0] = self.venv_dbt_path

        subprocess_result: FullOutputSubprocessResult = self.subprocess_hook.run_command(command, *args, **kwargs)
        return subprocess_result

    def execute(self, context: Context) -> None:
        output = super().execute(context)
        if self._venv_tmp_dir:
            self._venv_tmp_dir.cleanup()
        logger.info(output)

    def _get_or_create_venv_py_interpreter(self) -> str:
        """
        Helper method that parses virtual env configuration and returns a DBT binary within the resulting virtualenv:
        Do we have a persistent virtual env dir set in `self._venv_dir`?
        1. Yes: Does a directory at that path exist?
            1. No: Create it, and create a virtual env inside it
            2. Yes: Does the directory have a virtual env inside it?
                1. No: Create one in this directory and return it
                2. Yes: Return this virtual env
        2. No: Create a temporary virtual env and return it
        
        """
        if self._venv_dir is not None:
            if self._venv_dir.is_dir():
                py_interpreter_path = Path(f"{self._venv_dir}/bin/python")

                self.log.info(f"Checking for venv interpreter: {py_interpreter_path} : {py_interpreter_path.is_file()}")
                if py_interpreter_path.is_file():
                    self.log.info(f"Found Python interpreter in cached virtualenv: `{str(py_interpreter_path)}`")
                    return str(py_interpreter_path)
            else:
                os.mkdir(self._venv_dir)

            self.log.info(f"Creating virtualenv at `{self._venv_dir}")
            venv_directory = str(self._venv_dir)

        else:
            self.log.info("Creating temporary virtualenv")
            self._venv_tmp_dir = TemporaryDirectory(prefix="cosmos-venv")
            venv_directory = self._venv_tmp_dir.name

        return prepare_virtualenv(
            venv_directory=venv_directory,
            python_bin=PY_INTERPRETER,
            system_site_packages=self.py_system_site_packages,
            requirements=self.py_requirements,
        )


class DbtLSVirtualenvOperator(DbtVirtualenvBaseOperator, DbtLSLocalOperator):
    """
    Executes a dbt core ls command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """


class DbtSeedVirtualenvOperator(DbtVirtualenvBaseOperator, DbtSeedLocalOperator):
    """
    Executes a dbt core seed command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """


class DbtSnapshotVirtualenvOperator(DbtVirtualenvBaseOperator, DbtSnapshotLocalOperator):
    """
    Executes a dbt core snapshot command within a Python Virtual Environment, that is created before running the dbt
    command and deleted just after.
    """


class DbtRunVirtualenvOperator(DbtVirtualenvBaseOperator, DbtRunLocalOperator):
    """
    Executes a dbt core run command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """


class DbtTestVirtualenvOperator(DbtVirtualenvBaseOperator, DbtTestLocalOperator):
    """
    Executes a dbt core test command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """


class DbtRunOperationVirtualenvOperator(DbtVirtualenvBaseOperator, DbtRunOperationLocalOperator):
    """
    Executes a dbt core run-operation command within a Python Virtual Environment, that is created before running the
    dbt command and deleted just after.
    """


class DbtDocsVirtualenvOperator(DbtVirtualenvBaseOperator, DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command within a Python Virtual Environment, that is created before running the dbt
    command and deleted just after.
    """
