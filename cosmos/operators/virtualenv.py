from __future__ import annotations

import os
import shutil
import time
import traceback
from functools import cached_property
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Callable

import psutil
from airflow.utils.python_virtualenv import prepare_virtualenv

from cosmos.exceptions import CosmosValueError
from cosmos.hooks.subprocess import FullOutputSubprocessResult
from cosmos.operators.local import (
    DbtBuildLocalOperator,
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


PY_INTERPRETER = "python3"


def depends_on_virtualenv_dir(method: Callable[[Any], Any]) -> Callable[[Any], Any]:
    def wrapper(operator: DbtVirtualenvBaseOperator, *args: Any) -> None:
        if operator.virtualenv_dir is None:
            # TODO: test
            raise CosmosValueError(f"Method relies on value of parameter `virtualenv_dir` which is None.")

        method(operator, *args)

    return wrapper


class DbtVirtualenvBaseOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core cli command within a Python Virtual Environment, that is created before running the dbt command
    and deleted at the end of the operator execution.

    :param py_requirements: If defined, creates a virtual environment with the specified dependencies. Example:
           ["dbt-postgres==1.5.0"]
    :param pip_install_options: Pip options to use when installing Python dependencies. Example: ["--upgrade", "--no-cache-dir"]
    :param py_system_site_packages: Whether or not all the Python packages from the Airflow instance will be accessible
           within the virtual environment (if py_requirements argument is specified).
           Avoid using unless the dbt job requires it.
    """

    template_fields = DbtLocalBaseOperator.template_fields + ("virtualenv_dir", "is_virtualenv_dir_temporary")  # type: ignore[operator]

    def __init__(
        self,
        py_requirements: list[str] | None = None,
        pip_install_options: list[str] | None = None,
        py_system_site_packages: bool = False,
        virtualenv_dir: Path | None = None,
        is_virtualenv_dir_temporary: bool = False,
        **kwargs: Any,
    ) -> None:
        self.py_requirements = py_requirements or []
        self.pip_install_options = pip_install_options or []
        self.py_system_site_packages = py_system_site_packages
        self.virtualenv_dir = virtualenv_dir
        self.is_virtualenv_dir_temporary = is_virtualenv_dir_temporary
        super().__init__(**kwargs)

    @cached_property
    def venv_dbt_path(
        self,
    ) -> str:
        """
        Path to the dbt binary within a Python virtualenv.

        The first time this property is called, it creates a new/temporary and installs the dependencies
        based on the self.py_requirements, self.pip_install_options,  and self.py_system_site_packages,  or retrieves an existing virtualenv.
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

    def run_subprocess(self, command: list[str], env: dict[str, str], cwd: str) -> FullOutputSubprocessResult:
        if self.py_requirements:
            command[0] = self.venv_dbt_path

        subprocess_result: FullOutputSubprocessResult = self.subprocess_hook.run_command(
            command=command,
            env=env,
            cwd=cwd,
            output_encoding=self.output_encoding,
        )
        return subprocess_result

    def clean_dir_if_temporary(self) -> None:
        """
        Delete the virtualenv directory if it is temporary.
        """
        if self.is_virtualenv_dir_temporary and self.virtualenv_dir and self.virtualenv_dir.exists():
            # TODO: test
            self.log.info(f"Deleting the Python virtualenv {self.virtualenv_dir}")
            shutil.rmtree(str(self.virtualenv_dir), ignore_errors=True)

    def execute(self, context: Context) -> None:
        try:
            output = super().execute(context)
        except Exception:
            self.log.error(traceback.format_exc())
        else:
            self.log.info(output)
        finally:
            self.clean_dir_if_temporary()

    def on_kill(self) -> None:
        # TODO: test
        self.clean_dir_if_temporary()

    def prepare_virtualenv(self) -> str:
        self.log.info(f"Creating or updating the virtualenv at `{self.virtualenv_dir}")
        py_bin = prepare_virtualenv(
            venv_directory=str(self.virtualenv_dir),
            python_bin=PY_INTERPRETER,
            system_site_packages=self.py_system_site_packages,
            requirements=self.py_requirements,
            pip_install_options=self.pip_install_options,
        )
        return py_bin

    def _get_or_create_venv_py_interpreter(self) -> str:
        """Helper method that parses virtual env configuration
        and returns a DBT binary within the resulting virtualenv"""

        # No virtualenv_dir set, so create a temporary virtualenv
        if self.virtualenv_dir is None or self.is_virtualenv_dir_temporary:
            self.log.info("Creating temporary virtualenv")
            with TemporaryDirectory(prefix="cosmos-venv") as tempdir:
                self.virtualenv_dir = Path(tempdir)
                py_bin = self.prepare_virtualenv()
            return py_bin

        # Use a reusable virtualenv
        self.log.info(f"Checking if the virtualenv lock {str(self.__lock_file)} exists")
        while not self._is_lock_available():
            # TODO: test
            self.log.info("Waiting for virtualenv lock to be released")
            time.sleep(1)

        self.log.info(f"Acquiring the virtualenv lock")
        self.__acquire_venv_lock()
        py_bin = self.prepare_virtualenv()

        if not self.is_virtualenv_dir_temporary:
            self.log.info("Releasing virtualenv lock")
            self.__release_venv_lock()

        return py_bin

    @property
    def __lock_file(self) -> Path:
        return Path(f"{self.virtualenv_dir}/LOCK")

    @property
    def _pid(self) -> int:
        return os.getpid()

    # @depends_on_virtualenv_dir
    def _is_lock_available(self) -> bool:
        if self.__lock_file.is_file():
            with open(self.__lock_file) as lf:
                pid = int(lf.read())

                self.log.info(f"Checking for running process with PID {pid}")
                try:
                    _process_running = psutil.Process(pid).is_running()
                except psutil.NoSuchProcess:
                    # TODO: test
                    _process_running = False

                self.log.info(f"Process {pid} running: {_process_running}")
                # TODO: test
                return not _process_running

        return True

    @depends_on_virtualenv_dir
    def __acquire_venv_lock(self) -> None:
        if not self.virtualenv_dir.is_dir():  # type: ignore
            os.mkdir(str(self.virtualenv_dir))

        with open(self.__lock_file, "w") as lf:
            self.log.info(f"Acquiring lock at {self.__lock_file} with pid {str(self._pid)}")
            lf.write(str(self._pid))

    @depends_on_virtualenv_dir
    def __release_venv_lock(self) -> None:
        if not self.__lock_file.is_file():
            self.log.warn(f"Lockfile {self.__lock_file} not found, perhaps deleted by other concurrent operator?")
            return

        with open(self.__lock_file) as lf:
            # TODO: test
            lock_file_pid = int(lf.read())

            if lock_file_pid == self._pid:
                return self.__lock_file.unlink()

            # TODO: test
            # TODO: release lock if other process is not running
            self.log.warn(f"Lockfile owned by process of pid {lock_file_pid}, while operator has pid {self._pid}")


class DbtBuildVirtualenvOperator(DbtVirtualenvBaseOperator, DbtBuildLocalOperator):  # type: ignore[misc]
    """
    Executes a dbt core build command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """


class DbtLSVirtualenvOperator(DbtVirtualenvBaseOperator, DbtLSLocalOperator):
    """
    Executes a dbt core ls command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """


class DbtSeedVirtualenvOperator(DbtVirtualenvBaseOperator, DbtSeedLocalOperator):  # type: ignore[misc]
    """
    Executes a dbt core seed command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """


class DbtSnapshotVirtualenvOperator(DbtVirtualenvBaseOperator, DbtSnapshotLocalOperator):
    """
    Executes a dbt core snapshot command within a Python Virtual Environment, that is created before running the dbt
    command and deleted just after.
    """


class DbtRunVirtualenvOperator(DbtVirtualenvBaseOperator, DbtRunLocalOperator):  # type: ignore[misc]
    """
    Executes a dbt core run command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """


class DbtTestVirtualenvOperator(DbtVirtualenvBaseOperator, DbtTestLocalOperator):
    """
    Executes a dbt core test command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """


class DbtRunOperationVirtualenvOperator(DbtVirtualenvBaseOperator, DbtRunOperationLocalOperator):  # type: ignore[misc]
    """
    Executes a dbt core run-operation command within a Python Virtual Environment, that is created before running the
    dbt command and deleted just after.
    """


class DbtDocsVirtualenvOperator(DbtVirtualenvBaseOperator, DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command within a Python Virtual Environment, that is created before running the dbt
    command and deleted just after.
    """
