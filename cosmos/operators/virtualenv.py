from __future__ import annotations

import os
import shutil
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Callable, Sequence

import psutil

try:  # Airflow 3
    from airflow.providers.standard.utils.python_virtualenv import prepare_virtualenv
except ImportError:  # Airflow 2
    from airflow.utils.python_virtualenv import prepare_virtualenv  # type: ignore[no-redef]

from cosmos import settings
from cosmos.constants import InvocationMode
from cosmos.exceptions import CosmosValueError
from cosmos.hooks.subprocess import FullOutputSubprocessResult
from cosmos.log import get_logger
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtCloneLocalOperator,
    DbtDocsLocalOperator,
    DbtLocalBaseOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtSourceLocalOperator,
    DbtTestLocalOperator,
)

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]
    from dbt.cli.main import dbtRunnerResult

PY_INTERPRETER = "python3"
LOCK_FILENAME = "cosmos_virtualenv.lock"
logger = get_logger(__name__)


def depends_on_virtualenv_dir(method: Callable[[Any], Any]) -> Callable[[Any], Any]:
    def wrapper(operator: DbtVirtualenvBaseOperator, *args: Any) -> Any:
        if operator.virtualenv_dir is None:
            raise CosmosValueError(f"Method relies on value of parameter `virtualenv_dir` which is None.")
        return method(operator, *args)

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
    :param virtualenv_dir: Directory path where Cosmos will create/update Python virtualenv. If defined, will persist the Python virtualenv in the Airflow worker node.
    :param is_virtualenv_dir_temporary: Tells Cosmos if virtualenv should be persisted or not.
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
        self.virtualenv_dir = Path(virtualenv_dir) if virtualenv_dir else None
        if self.virtualenv_dir:
            self.virtualenv_dir.mkdir(parents=True, exist_ok=True)
        self.is_virtualenv_dir_temporary = is_virtualenv_dir_temporary
        self.max_retries_lock = settings.virtualenv_max_retries_lock
        self._py_bin: str | None = None
        kwargs["invocation_mode"] = InvocationMode.SUBPROCESS
        super().__init__(**kwargs)
        if not self.py_requirements:
            self.log.error("Cosmos virtualenv operators require the `py_requirements` parameter")

    def run_subprocess(self, command: list[str], env: dict[str, str], cwd: str) -> FullOutputSubprocessResult:
        if self._py_bin is not None:
            self.log.info(f"Using Python binary from virtualenv: {self._py_bin}")
            command[0] = str(Path(self._py_bin).parent / "dbt")
        return super().run_subprocess(command, env, cwd)

    def run_command(
        self,
        cmd: list[str],
        env: dict[str, str | bytes | os.PathLike[Any]],
        context: Context,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        push_run_results_to_xcom: bool = False,
    ) -> FullOutputSubprocessResult | dbtRunnerResult:
        # No virtualenv_dir set, so create a temporary virtualenv
        if self.virtualenv_dir is None or self.is_virtualenv_dir_temporary:
            self.log.info("Creating temporary virtualenv")
            with TemporaryDirectory(prefix="cosmos-venv") as tempdir:
                self.virtualenv_dir = Path(tempdir)
                self._py_bin = self._prepare_virtualenv()
                return super().run_command(
                    cmd,
                    env,
                    context,
                    run_as_async=run_as_async,
                    async_context=async_context,
                    push_run_results_to_xcom=push_run_results_to_xcom,
                )

        try:
            self.log.info(f"Checking if the virtualenv lock {str(self._lock_file)} exists")
            while not self._is_lock_available() and self.max_retries_lock:
                logger.info("Waiting for virtualenv lock to be released")
                time.sleep(1)
                self.max_retries_lock -= 1

            self.log.info("Acquiring the virtualenv lock")
            self._acquire_venv_lock()
            self._py_bin = self._prepare_virtualenv()
            return super().run_command(
                cmd,
                env,
                context,
                run_as_async=run_as_async,
                async_context=async_context,
                push_run_results_to_xcom=push_run_results_to_xcom,
            )
        finally:
            self.log.info("Releasing virtualenv lock")
            self._release_venv_lock()

    def clean_dir_if_temporary(self) -> None:
        """
        Delete the virtualenv directory if it is temporary.
        """
        if self.is_virtualenv_dir_temporary and self.virtualenv_dir and self.virtualenv_dir.exists():
            self.log.info(f"Deleting the Python virtualenv {self.virtualenv_dir}")
            shutil.rmtree(str(self.virtualenv_dir), ignore_errors=True)

    def execute(self, context: Context, **kwargs: Any) -> None:
        try:
            output = super().execute(context)
            self.log.info(output)
        finally:
            self.clean_dir_if_temporary()

    def on_kill(self) -> None:
        self.clean_dir_if_temporary()

    def _prepare_virtualenv(self) -> Any:
        self.log.info(f"Creating or updating the virtualenv at `{self.virtualenv_dir}")
        py_bin = prepare_virtualenv(
            venv_directory=str(self.virtualenv_dir),
            python_bin=PY_INTERPRETER,
            system_site_packages=self.py_system_site_packages,
            requirements=self.py_requirements,
            pip_install_options=self.pip_install_options,
        )
        return py_bin

    @property
    def _lock_file(self) -> Path:
        filepath = Path(f"{self.virtualenv_dir}/{LOCK_FILENAME}")
        return filepath

    @property
    def _pid(self) -> int:
        return os.getpid()

    @depends_on_virtualenv_dir
    def _is_lock_available(self) -> bool:
        is_available = True
        if self._lock_file.is_file():
            with open(self._lock_file) as lf:
                pid = int(lf.read())
                self.log.info(f"Checking for running process with PID {pid}")
                try:
                    _process_running = psutil.Process(pid).is_running()
                    self.log.info(f"Process {pid} running: {_process_running} and has the lock {self._lock_file}.")
                except psutil.NoSuchProcess:
                    self.log.info(f"Process {pid} is not running. Lock {self._lock_file} was outdated.")
                    is_available = True
                else:
                    is_available = not _process_running
        return is_available

    @depends_on_virtualenv_dir
    def _acquire_venv_lock(self) -> None:
        if not self.virtualenv_dir.is_dir():  # type: ignore
            os.mkdir(str(self.virtualenv_dir))

        with open(self._lock_file, "w") as lf:
            logger.info(f"Acquiring lock at {self._lock_file} with pid {str(self._pid)}")
            lf.write(str(self._pid))

    @depends_on_virtualenv_dir
    def _release_venv_lock(self) -> None:
        if not self._lock_file.is_file():
            logger.warning(f"Lockfile {self._lock_file} not found, perhaps deleted by other concurrent operator?")
            return

        with open(self._lock_file) as lf:
            lock_file_pid = int(lf.read())

            if lock_file_pid == self._pid:
                return self._lock_file.unlink()

            logger.warning(f"Lockfile owned by process of pid {lock_file_pid}, while operator has pid {self._pid}")


class DbtBuildVirtualenvOperator(DbtVirtualenvBaseOperator, DbtBuildLocalOperator):  # type: ignore[misc]
    """
    Executes a dbt core build command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtLSVirtualenvOperator(DbtVirtualenvBaseOperator, DbtLSLocalOperator):
    """
    Executes a dbt core ls command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """

    template_fields: Sequence[str] = DbtVirtualenvBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtSeedVirtualenvOperator(DbtVirtualenvBaseOperator, DbtSeedLocalOperator):  # type: ignore[misc]
    """
    Executes a dbt core seed command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtSnapshotVirtualenvOperator(DbtVirtualenvBaseOperator, DbtSnapshotLocalOperator):
    """
    Executes a dbt core snapshot command within a Python Virtual Environment, that is created before running the dbt
    command and deleted just after.
    """

    template_fields: Sequence[str] = DbtVirtualenvBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtSourceVirtualenvOperator(DbtVirtualenvBaseOperator, DbtSourceLocalOperator):
    """
    Executes `dbt source freshness` command within a Python Virtual Environment, that is created before running the dbt
    command and deleted just after.
    """

    template_fields: Sequence[str] = DbtVirtualenvBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtRunVirtualenvOperator(DbtVirtualenvBaseOperator, DbtRunLocalOperator):  # type: ignore[misc]
    """
    Executes a dbt core run command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtTestVirtualenvOperator(DbtVirtualenvBaseOperator, DbtTestLocalOperator):
    """
    Executes a dbt core test command within a Python Virtual Environment, that is created before running the dbt command
    and deleted just after.
    """

    template_fields: Sequence[str] = DbtVirtualenvBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtRunOperationVirtualenvOperator(DbtVirtualenvBaseOperator, DbtRunOperationLocalOperator):  # type: ignore[misc]
    """
    Executes a dbt core run-operation command within a Python Virtual Environment, that is created before running the
    dbt command and deleted just after.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtDocsVirtualenvOperator(DbtVirtualenvBaseOperator, DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command within a Python Virtual Environment, that is created before running the dbt
    command and deleted just after.
    """

    template_fields: Sequence[str] = DbtVirtualenvBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtCloneVirtualenvOperator(DbtVirtualenvBaseOperator, DbtCloneLocalOperator):
    """
    Executes a dbt core clone command.
    """

    template_fields: Sequence[str] = DbtVirtualenvBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
