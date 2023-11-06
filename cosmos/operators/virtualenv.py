from __future__ import annotations

import os
import psutil
import time
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
    template_fields = DbtLocalBaseOperator.template_fields + ("virtualenv_dir",)

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
        self.virtualenv_dir = virtualenv_dir
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
        """Helper method that parses virtual env configuration 
        and returns a DBT binary within the resulting virtualenv"""

        # No virtualenv_dir set, so revert to making a temporary virtualenv
        if self.virtualenv_dir is None:
            self.log.info("Creating temporary virtualenv")
            self._venv_tmp_dir = TemporaryDirectory(prefix="cosmos-venv")

            return prepare_virtualenv(
                venv_directory=self._venv_tmp_dir.name,
                python_bin=PY_INTERPRETER,
                system_site_packages=self.py_system_site_packages,
                requirements=self.py_requirements,
            )

        self.log.info(f"Checking if {str(self._lock_file)} exists")
        while not self._is_lock_available():
            self.log.info("Waiting for lock to release")
            time.sleep(1)

        self.log.info(f"Creating virtualenv at `{self.virtualenv_dir}")
        self.log.info(f"Acquiring available lock")
        self._acquire_venv_lock()

        py_bin = prepare_virtualenv(
            venv_directory=str(self.virtualenv_dir),
            python_bin=PY_INTERPRETER,
            system_site_packages=self.py_system_site_packages,
            requirements=self.py_requirements,
        )

        self.log.info("Releasing lock")
        self._release_venv_lock()

        return py_bin
    
    @property
    def _lock_file(self) -> Path:
        return Path(f"{self.virtualenv_dir}/LOCK")
    
    def _is_lock_available(self) -> bool:
        if self._lock_file.is_file():
            with open(self._lock_file, "r") as lf:
                pid = int(lf.read())

                self.log.info(f"Checking for running process with PID {pid}")
                _process_running = psutil.Process(pid).is_running()

                self.log.info(f"Process {pid} running: {_process_running}")
                return not _process_running

        return True

    def _acquire_venv_lock(self) -> None:
        if not self.virtualenv_dir.is_dir():
            os.mkdir(str(self.virtualenv_dir))

        with open(self._lock_file, "w") as lf:
            pid = str(os.getpid())
            self.log.info(f"Acquiring lock at {self._lock_file} with pid {pid}")
            lf.write(pid)
        
    def _release_venv_lock(self) -> None:
        self._lock_file.unlink()


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

