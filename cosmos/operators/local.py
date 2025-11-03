from __future__ import annotations

import base64
import inspect
import json
import os
import tempfile
import time
import urllib.parse
import warnings
import zlib
from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal, Sequence
from urllib.parse import urlparse

import airflow
import jinja2
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.taskinstance import TaskInstance

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

from airflow.version import version as airflow_version
from attrs import define
from packaging.version import Version

from cosmos import cache, settings

if settings.AIRFLOW_IO_AVAILABLE:
    try:
        from airflow.sdk import ObjectStoragePath
    except ImportError:
        from airflow.io.path import ObjectStoragePath
from cosmos._utils.importer import load_method_from_module
from cosmos.cache import (
    _copy_cached_package_lockfile_to_project,
    _get_latest_cached_package_lockfile,
    is_cache_package_lockfile_enabled,
)
from cosmos.constants import (
    _AIRFLOW3_MAJOR_VERSION,
    DBT_DEPENDENCIES_FILE_NAMES,
    FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP,
    InvocationMode,
)
from cosmos.dataset import get_dataset_alias_name
from cosmos.dbt.project import (
    copy_dbt_packages,
    copy_manifest_file_if_exists,
    get_partial_parse_path,
    has_non_empty_dependencies_file,
)
from cosmos.exceptions import AirflowCompatibilityError, CosmosDbtRunError, CosmosValueError
from cosmos.settings import (
    remote_target_path,
    remote_target_path_conn_id,
)

try:
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3
except ImportError:
    from airflow.models import BaseOperator  # Airflow 2

try:  # Airflow 3
    from airflow.sdk.definitions.asset import Asset
except (ModuleNotFoundError, ImportError):  # Airflow 2
    from airflow.datasets import Dataset as Asset  # type: ignore


if TYPE_CHECKING:  # pragma: no cover
    import openlineage  # pragma: no cover
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    try:  # pragma: no cover
        from openlineage.client.event_v2 import RunEvent  # pragma: no cover
    except ImportError:  # pragma: no cover
        from openlineage.client.run import RunEvent  # pragma: no cover


from sqlalchemy.orm import Session

import cosmos.dbt.runner as dbt_runner
from cosmos.config import ProfileConfig
from cosmos.constants import (
    OPENLINEAGE_PRODUCER,
)
from cosmos.dbt.parser.output import (
    extract_freshness_warn_msg,
    extract_log_issues,
    parse_number_of_warnings_subprocess,
)
from cosmos.dbt.project import create_symlinks
from cosmos.hooks.subprocess import (
    FullOutputSubprocessHook,
    FullOutputSubprocessResult,
)
from cosmos.log import get_logger
from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtCloneMixin,
    DbtCompileMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
    _sanitize_xcom_key,
)

AIRFLOW_VERSION = Version(airflow.__version__)

logger = get_logger(__name__)


# The following is related to the ability of Cosmos parsing dbt artifacts and generating OpenLineage URIs
# It is used for emitting Airflow assets and not necessarily OpenLineage events
try:
    import openlineage
    from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor

    is_openlineage_common_available = True
except ModuleNotFoundError:
    is_openlineage_common_available = False
    DbtLocalArtifactProcessor = None


# The following is related to the ability of Airflow to emit OpenLineage events
# This will decide if the method `get_openlineage_facets_on_complete` will be called by the Airflow OpenLineage listener or not
try:
    from airflow.providers.openlineage.extractors.base import OperatorLineage
except (ImportError, ModuleNotFoundError):
    try:
        from openlineage.airflow.extractors.base import OperatorLineage
    except (ImportError, ModuleNotFoundError):
        logger.warning(
            "To enable emitting Openlineage events, upgrade to Airflow 2.7 or install astronomer-cosmos[openlineage]."
        )
        logger.debug(
            "Further details on lack of Openlineage Airflow provider:",
            stack_info=True,
        )

        @define
        class OperatorLineage:  # type: ignore
            inputs: list[str] = list()
            outputs: list[str] = list()
            run_facets: dict[str, str] = dict()
            job_facets: dict[str, str] = dict()


class AbstractDbtLocalBase(AbstractDbtBase):
    """
    Executes a dbt core cli command locally.

    :param profile_args: Arguments to pass to the profile. See
        :py:class:`cosmos.providers.dbt.core.profiles.BaseProfileMapping`.
    :param profile_config: ProfileConfig Object
    :param install_deps (deprecated): If true, install dependencies before running the command
    :param copy_dbt_packages: If true, copy pre-existing `dbt_packages` (before running dbt deps)
    :param callback: A callback function called on after a dbt run with a path to the dbt project directory.
    :param manifest_filepath: The path to the user-defined Manifest file. It's "" by default.
    :param target_name: A name to use for the dbt target. If not provided, and no target is found
        in your project's dbt_project.yml, "cosmos_target" is used.
    :param should_store_compiled_sql: If true, store the compiled SQL in the compiled_sql rendered template.
    :param append_env: If True(default), inherits the environment variables
        from current process and then environment variable passed by the user will either update the existing
        inherited environment variables or the new variables gets appended to it.
        If False, only uses the environment variables passed in env params
        and does not inherit the current process environment.
    """

    template_fields: Sequence[str] = AbstractDbtBase.template_fields + ("compiled_sql", "freshness")  # type: ignore[operator]
    template_fields_renderers = {
        "compiled_sql": "sql",
        "freshness": "json",
    }

    def __init__(
        self,
        task_id: str,
        profile_config: ProfileConfig,
        invocation_mode: InvocationMode | None = None,
        install_deps: bool = True,
        copy_dbt_packages: bool = settings.default_copy_dbt_packages,
        manifest_filepath: str = "",
        callback: Callable[[str], None] | list[Callable[[str], None]] | None = None,
        callback_args: dict[str, Any] | None = None,
        should_store_compiled_sql: bool = True,
        should_upload_compiled_sql: bool = False,
        append_env: bool = True,
        dbt_runner_callbacks: list[Callable] | None = None,  # type: ignore[type-arg]
        **kwargs: Any,
    ) -> None:
        self.task_id = task_id
        self.profile_config = profile_config
        self.callback = callback
        self.callback_args = callback_args or {}
        self.compiled_sql = ""
        self.freshness = ""
        self.should_store_compiled_sql = should_store_compiled_sql
        self.should_upload_compiled_sql = should_upload_compiled_sql
        self.openlineage_events_completes: list[RunEvent] = []
        self.invocation_mode = invocation_mode
        self._dbt_runner: dbtRunner | None = None
        self._dbt_runner_callbacks = dbt_runner_callbacks

        super().__init__(task_id=task_id, **kwargs)

        # For local execution mode, we're consistent with the LoadMode.DBT_LS command in forwarding the environment
        # variables to the subprocess by default. Although this behavior is designed for ExecuteMode.LOCAL and
        # ExecuteMode.VIRTUALENV, it is not desired for the other execution modes to forward the environment variables
        # as it can break existing DAGs.
        self.append_env = append_env

        # We should not spend time trying to install deps if the project doesn't have any dependencies
        self.install_deps = install_deps and has_non_empty_dependencies_file(Path(self.project_dir))
        self.copy_dbt_packages = copy_dbt_packages

        self.manifest_filepath = manifest_filepath

    @cached_property
    def subprocess_hook(self) -> FullOutputSubprocessHook:
        """Returns hook for running the bash command."""
        return FullOutputSubprocessHook()

    @property
    def invoke_dbt(self) -> Callable[..., FullOutputSubprocessResult | dbtRunnerResult]:
        if self.invocation_mode == InvocationMode.SUBPROCESS:
            return self.run_subprocess
        elif self.invocation_mode == InvocationMode.DBT_RUNNER:
            return self.run_dbt_runner
        else:
            raise ValueError(f"Invalid invocation mode: {self.invocation_mode}")

    @property
    def handle_exception(self) -> Callable[..., None]:
        if self.invocation_mode == InvocationMode.SUBPROCESS:
            return self.handle_exception_subprocess
        elif self.invocation_mode == InvocationMode.DBT_RUNNER:
            return self.handle_exception_dbt_runner
        else:
            raise ValueError(f"Invalid invocation mode: {self.invocation_mode}")

    def _discover_invocation_mode(self) -> None:
        """Discovers the invocation mode based on the availability of dbtRunner for import. If dbtRunner is available, it will
        be used since it is faster than subprocess. If dbtRunner is not available, it will fall back to subprocess.
        This method is called at runtime to work in the environment where the operator is running.
        """
        if dbt_runner.is_available():
            self.invocation_mode = InvocationMode.DBT_RUNNER
            logger.info("dbtRunner is available. Using dbtRunner for invoking dbt.")
        else:
            self.invocation_mode = InvocationMode.SUBPROCESS
            logger.info("Could not import dbtRunner. Falling back to subprocess for invoking dbt.")

    def handle_exception_subprocess(self, result: FullOutputSubprocessResult) -> None:
        if self.skip_exit_code is not None and result.exit_code == self.skip_exit_code:
            raise AirflowSkipException(f"dbt command returned exit code {self.skip_exit_code}. Skipping.")
        elif result.exit_code != 0:
            logger.error("\n".join(result.full_output))
            raise AirflowException(f"dbt command failed. The command returned a non-zero exit code {result.exit_code}.")

    def handle_exception_dbt_runner(self, result: dbtRunnerResult) -> None:
        """dbtRunnerResult has an attribute `success` that is False if the command failed."""
        return dbt_runner.handle_exception_if_needed(result)

    def store_compiled_sql(self, tmp_project_dir: str, context: Context) -> None:
        """
        Takes the compiled SQL files from the dbt run and stores them in the compiled_sql rendered template.
        Gets called after every dbt run.
        """
        if not self.should_store_compiled_sql:
            return

        compiled_queries = {}
        # dbt compiles sql files and stores them in the target directory
        for folder_path, _, file_paths in os.walk(os.path.join(tmp_project_dir, "target")):
            for file_path in file_paths:
                if not file_path.endswith(".sql"):
                    continue

                compiled_sql_path = Path(os.path.join(folder_path, file_path))
                compiled_sql = compiled_sql_path.read_text(encoding="utf-8")

                relative_path = str(compiled_sql_path.relative_to(tmp_project_dir))
                compiled_queries[relative_path] = compiled_sql.strip()

        for name, query in compiled_queries.items():
            self.compiled_sql += f"-- {name}\n{query}\n\n"

        self.compiled_sql = self.compiled_sql.strip()

    @staticmethod
    def _configure_remote_target_path() -> tuple[Path | ObjectStoragePath, str] | tuple[None, None]:
        """Configure the remote target path if it is provided."""
        if not remote_target_path:
            return None, None

        _configured_target_path = None

        target_path_str = str(remote_target_path)

        remote_conn_id = remote_target_path_conn_id
        if not remote_conn_id:
            target_path_schema = urlparse(target_path_str).scheme
            remote_conn_id = FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP.get(target_path_schema, None)  # type: ignore[assignment]
        if remote_conn_id is None:
            logger.info(
                "Remote target connection not set. Please, configure [cosmos][remote_target_path_conn_id] or set the environment variable AIRFLOW__COSMOS__REMOTE_TARGET_PATH_CONN_ID"
            )
            return None, None

        if not settings.AIRFLOW_IO_AVAILABLE:
            raise CosmosValueError(
                f"You're trying to specify remote target path {target_path_str}, but the required "
                f"Object Storage feature is unavailable in Airflow version {airflow_version}. Please upgrade to "
                "Airflow 2.8 or later."
            )

        _configured_target_path = ObjectStoragePath(target_path_str, conn_id=remote_conn_id)

        if not _configured_target_path.exists():  # type: ignore[no-untyped-call]
            _configured_target_path.mkdir(parents=True, exist_ok=True)

        return _configured_target_path, remote_conn_id

    def _construct_dest_file_path(
        self, dest_target_dir: Path | ObjectStoragePath, file_path: str, source_compiled_dir: Path, resource_type: str
    ) -> str:
        """
        Construct the destination path for the compiled SQL files to be uploaded to the remote store.
        """
        dest_target_dir_str = str(dest_target_dir).rstrip("/")
        dag_task_group_identifier = self.extra_context["dbt_dag_task_group_identifier"]
        rel_path = os.path.relpath(file_path, source_compiled_dir).lstrip("/")
        run_id = self.extra_context["run_id"]

        if settings.upload_sql_to_xcom:
            return f"{dag_task_group_identifier}/{run_id}/{resource_type}/{rel_path}"

        return f"{dest_target_dir_str}/{dag_task_group_identifier}/{run_id}/{resource_type}/{rel_path}"

    def _upload_sql_files(self, tmp_project_dir: str, resource_type: str) -> None:
        start_time = time.time()

        dest_target_dir, dest_conn_id = self._configure_remote_target_path()

        if not dest_target_dir:
            raise CosmosValueError("You're trying to upload SQL files, but the remote target path is not configured. ")

        source_run_dir = Path(tmp_project_dir) / f"target/{resource_type}"
        files = [str(file) for file in source_run_dir.rglob("*") if file.is_file()]
        for file_path in files:
            dest_file_path = self._construct_dest_file_path(dest_target_dir, file_path, source_run_dir, resource_type)
            dest_object_storage_path = ObjectStoragePath(dest_file_path, conn_id=dest_conn_id)
            dest_object_storage_path.parent.mkdir(parents=True, exist_ok=True)
            ObjectStoragePath(file_path).copy(dest_object_storage_path)
            logger.debug("Copied %s to %s", file_path, dest_object_storage_path)

        elapsed_time = time.time() - start_time
        logger.info("SQL files upload completed in %.2f seconds.", elapsed_time)

    def _upload_sql_files_xcom(self, context: Context, tmp_project_dir: str, resource_type: str) -> None:
        start_time = time.time()
        source_run_dir = Path(tmp_project_dir) / f"target/{resource_type}"
        files = [str(file) for file in source_run_dir.rglob("*") if file.is_file()]
        for file_path in files:
            sql_model_path = os.path.relpath(file_path, source_run_dir).lstrip("/")
            with open(file_path, encoding="utf-8") as f:
                sql_query = f.read()
            compressed_sql = zlib.compress(sql_query.encode("utf-8"))
            compressed_b64_sql = base64.b64encode(compressed_sql).decode("utf-8")
            context["ti"].xcom_push(key=_sanitize_xcom_key(sql_model_path), value=compressed_b64_sql)
            logger.debug("SQL files %s uploaded to xcom.", sql_model_path)

        elapsed_time = time.time() - start_time
        logger.info("SQL files upload to xcom completed in %.2f seconds.", elapsed_time)

    def _delete_sql_files(self) -> None:
        """Deletes the entire run-specific directory from the remote target."""
        dest_target_dir, dest_conn_id = self._configure_remote_target_path()
        if not dest_target_dir or not dest_conn_id:
            logger.warning("Remote target path or connection ID not configured. Skipping deletion.")
            return

        dag_task_group_identifier = self.extra_context["dbt_dag_task_group_identifier"]
        run_id = self.extra_context["run_id"]
        run_dir_path_str = f"{str(dest_target_dir).rstrip('/')}/{dag_task_group_identifier}/{run_id}"
        run_dir_path = ObjectStoragePath(run_dir_path_str, conn_id=dest_conn_id)

        if run_dir_path.exists():
            run_dir_path.rmdir(recursive=True)
            logger.info("Deleted remote run directory: %s", run_dir_path_str)
        else:
            logger.debug("Remote run directory does not exist, skipping deletion: %s", run_dir_path_str)

    def store_freshness_json(self, tmp_project_dir: str, context: Context) -> None:
        """
        Takes the compiled sources.json file from the dbt source freshness and stores it in the freshness rendered template.
        Gets called after every dbt run / source freshness.
        """
        if not self.should_store_compiled_sql:
            return

        sources_json_path = Path(os.path.join(tmp_project_dir, "target", "sources.json"))
        if sources_json_path.exists():
            sources_json_content = sources_json_path.read_text(encoding="utf-8").strip()
            sources_data = json.loads(sources_json_content)
            formatted_sources_json = json.dumps(sources_data, indent=4)
            self.freshness = formatted_sources_json
        else:
            self.freshness = ""

    def _override_rtif(self, context: Context) -> None:
        if not self.should_store_compiled_sql:
            return

        if AIRFLOW_VERSION.major == _AIRFLOW3_MAJOR_VERSION:
            self.overwrite_rtif_after_execution = True
            return

        # Block to override the RTIFs in Airflow 2.x
        from airflow.utils.session import NEW_SESSION, provide_session

        @provide_session
        def _override_rtif_airflow_2_x(session: Session = NEW_SESSION) -> None:
            # need to refresh the rendered task field record in the db because Airflow only does this
            # before executing the task, not after
            from airflow.models.renderedtifields import RenderedTaskInstanceFields

            ti = context["ti"]

            if isinstance(
                ti, TaskInstance
            ):  # verifies ti is a TaskInstance in order to access and use the "task" field
                if TYPE_CHECKING:
                    assert ti.task is not None
                ti.task.template_fields = self.template_fields
                rtif = RenderedTaskInstanceFields(ti, render_templates=False)

                # delete the old records
                session.query(RenderedTaskInstanceFields).filter(
                    RenderedTaskInstanceFields.dag_id == self.dag_id,  # type: ignore[attr-defined]
                    RenderedTaskInstanceFields.task_id == self.task_id,
                    RenderedTaskInstanceFields.run_id == ti.run_id,
                ).delete()
                session.add(rtif)
            else:
                logger.info("Warning: ti is of type TaskInstancePydantic. Cannot update template_fields.")

        _override_rtif_airflow_2_x()

    def run_subprocess(self, command: list[str], env: dict[str, str], cwd: str) -> FullOutputSubprocessResult:
        logger.info("Trying to run the command:\n %s\nFrom %s", command, cwd)
        subprocess_result: FullOutputSubprocessResult = self.subprocess_hook.run_command(
            command=command,
            env=env,
            cwd=cwd,
            output_encoding=self.output_encoding,
        )
        # Logging changed in Airflow 3.1 and we needed to replace the output by the full output:
        output = "".join(subprocess_result.full_output)
        logger.info(output)
        return subprocess_result

    def run_dbt_runner(self, command: list[str], env: dict[str, str], cwd: str) -> dbtRunnerResult:
        """Invokes the dbt command programmatically."""
        if not dbt_runner.is_available():
            raise CosmosDbtRunError(
                "Could not import dbt core. Ensure that dbt-core >= v1.5 is installed and available in the environment where the operator is running."
            )

        return dbt_runner.run_command(command, env, cwd, callbacks=self._dbt_runner_callbacks)

    def _cache_package_lockfile(self, tmp_project_dir: Path) -> None:
        project_dir = Path(self.project_dir)
        if is_cache_package_lockfile_enabled(project_dir):
            latest_package_lockfile = _get_latest_cached_package_lockfile(project_dir)
            if latest_package_lockfile:
                _copy_cached_package_lockfile_to_project(latest_package_lockfile, tmp_project_dir)

    def _read_run_sql_from_target_dir(self, tmp_project_dir: str, sql_context: dict[str, Any]) -> str:
        package_name = sql_context.get("package_name") or Path(self.project_dir).name
        sql_relative_path = sql_context["dbt_node_config"]["file_path"].split(package_name)[-1].lstrip("/")
        run_sql_path = Path(tmp_project_dir) / "target/run" / Path(package_name).name / sql_relative_path

        with run_sql_path.open("r") as sql_file:
            sql_content: str = sql_file.read()
        return sql_content

    def _clone_project(self, tmp_dir_path: Path) -> None:
        logger.info(
            "Cloning project to writable temp directory %s from %s",
            tmp_dir_path,
            self.project_dir,
        )
        should_not_create_dbt_deps_symbolic_link = self.install_deps or self.copy_dbt_packages
        create_symlinks(
            Path(self.project_dir), tmp_dir_path, ignore_dbt_packages=should_not_create_dbt_deps_symbolic_link
        )
        if self.copy_dbt_packages:
            logger.info("Copying dbt packages to temporary folder.")
            copy_dbt_packages(Path(self.project_dir), tmp_dir_path)
            logger.info("Completed copying dbt packages to temporary folder.")

        copy_manifest_file_if_exists(self.manifest_filepath, Path(tmp_dir_path))

    def _handle_partial_parse(self, tmp_dir_path: Path) -> None:
        if self.cache_dir is None:
            return
        latest_partial_parse = cache._get_latest_partial_parse(Path(self.project_dir), self.cache_dir)
        logger.info("Partial parse is enabled and the latest partial parse file is %s", latest_partial_parse)
        if latest_partial_parse is not None:
            cache._copy_partial_parse_to_project(latest_partial_parse, tmp_dir_path)

    def _generate_dbt_flags(self, tmp_project_dir: str, profile_path: Path) -> list[str]:
        dbt_flags = [
            "--project-dir",
            str(tmp_project_dir),
            "--profiles-dir",
            str(profile_path.parent),
            "--profile",
            self.profile_config.profile_name,
            "--target",
            self.profile_config.target_name,
        ]
        if self.invocation_mode == InvocationMode.DBT_RUNNER:
            from dbt.version import __version__ as dbt_version

            if Version(dbt_version) >= Version("1.5.6"):
                # PR #1484 introduced the use of dbtRunner during DAG parsing. As a result, invoking dbtRunner again
                # during task execution can lead to task hangs—especially on Airflow 2.x. Investigation revealed that
                # the issue stems from how dbtRunner handles static parsing. Cosmos copies the dbt project to temporary
                # directories, and the use of different temp paths between parsing and execution appears to interfere
                # with dbt's static parsing behavior. As a workaround, passing the --no-static-parser flag avoids these
                # hangs and ensures reliable task execution.
                dbt_flags.append("--no-static-parser")
        return dbt_flags

    def _install_dependencies(
        self, tmp_dir_path: Path, flags: list[str], env: dict[str, str | bytes | os.PathLike[Any]]
    ) -> None:
        self._cache_package_lockfile(tmp_dir_path)
        deps_command = [self.dbt_executable_path, "deps"] + flags

        for filename in DBT_DEPENDENCIES_FILE_NAMES:
            filepath = tmp_dir_path / filename
            if filepath.is_file():
                logger.debug("Checking for the %s dependencies file.", str(filename))
                logger.debug("Contents of the <%s> dependencies file:\n %s", str(filepath), str(filepath.read_text()))

        self.invoke_dbt(command=deps_command, env=env, cwd=tmp_dir_path)

    @staticmethod
    def _mock_dbt_adapter(async_context: dict[str, Any] | None) -> None:
        if not async_context:
            raise CosmosValueError("`async_context` is necessary for running the model asynchronously")
        if "profile_type" not in async_context:
            raise CosmosValueError("`profile_type` needs to be specified in `async_context` when running as async")
        profile_type = async_context["profile_type"]
        module_path = f"cosmos.operators._asynchronous.{profile_type}"
        method_name = f"_mock_{profile_type}_adapter"
        mock_adapter_callable = load_method_from_module(module_path, method_name)
        mock_adapter_callable()

    def _handle_datasets(self, context: Context) -> None:
        inlets = self.get_datasets("inputs")
        outlets = self.get_datasets("outputs")
        logger.info("Inlets: %s", inlets)
        logger.info("Outlets: %s", outlets)
        self.register_dataset(inlets, outlets, context)

    def _update_partial_parse_cache(self, tmp_dir_path: Path) -> None:
        if self.cache_dir is None:
            return
        partial_parse_file = get_partial_parse_path(tmp_dir_path)
        if partial_parse_file.exists():
            cache._update_partial_parse_cache(partial_parse_file, self.cache_dir)

    def _push_run_results_to_xcom(self, tmp_project_dir: str, context: Context) -> None:
        run_results_path = Path(tmp_project_dir) / "target" / "run_results.json"
        if not run_results_path.is_file():
            raise AirflowException(f"run_results.json not found at {run_results_path}")

        try:
            with run_results_path.open() as fp:
                raw = json.load(fp)
        except json.JSONDecodeError as exc:
            raise AirflowException("Invalid JSON in run_results.json") from exc
        logger.debug("Loaded run results from %s", run_results_path)

        compressed = base64.b64encode(zlib.compress(json.dumps(raw).encode())).decode()
        context["ti"].xcom_push(key="run_results", value=compressed)

        logger.info("Pushed run results to XCom")

    def _handle_post_execution(
        self, tmp_project_dir: str, context: Context, push_run_results_to_xcom: bool = False
    ) -> None:
        self.store_freshness_json(tmp_project_dir, context)
        self.store_compiled_sql(tmp_project_dir, context)
        self._override_rtif(context)

        if self.should_upload_compiled_sql:
            self._upload_sql_files(tmp_project_dir, "compiled")

        if push_run_results_to_xcom:
            self._push_run_results_to_xcom(tmp_project_dir, context)

        if self.callback:
            self.callback_args.update({"context": context})
            if isinstance(self.callback, list):
                for callback_fn in self.callback:
                    callback_fn(tmp_project_dir, **self.callback_args)
            else:
                self.callback(tmp_project_dir, **self.callback_args)

    def _handle_async_execution(self, tmp_project_dir: str, context: Context, async_context: dict[str, Any]) -> None:
        if settings.enable_setup_async_task:
            if settings.upload_sql_to_xcom:
                self._upload_sql_files_xcom(context, tmp_project_dir, "run")
            else:
                self._upload_sql_files(tmp_project_dir, "run")
        else:
            sql = self._read_run_sql_from_target_dir(tmp_project_dir, async_context)
            profile_type = async_context["profile_type"]
            module_path = f"cosmos.operators._asynchronous.{profile_type}"
            method_name = f"_configure_{profile_type}_async_op_args"
            async_op_configurator = load_method_from_module(module_path, method_name)
            async_op_configurator(self, sql=sql)
            async_context["async_operator"].execute(self, context)

    def run_command(  # noqa: C901
        self,
        cmd: list[str],
        env: dict[str, str | bytes | os.PathLike[Any]],
        context: Context,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        push_run_results_to_xcom: bool = False,
    ) -> FullOutputSubprocessResult | dbtRunnerResult | str:
        """
        Copies the dbt project to a temporary directory and runs the command.
        """
        if not self.invocation_mode:
            self._discover_invocation_mode()

        if self.extra_context.get("run_id") is None:
            self.extra_context["run_id"] = context["run_id"]

        with tempfile.TemporaryDirectory() as tmp_project_dir:
            tmp_dir_path = Path(tmp_project_dir)
            env = {k: str(v) for k, v in env.items()}

            self._clone_project(tmp_dir_path)

            if self.partial_parse:
                self._handle_partial_parse(tmp_dir_path)

            with self.profile_config.ensure_profile() as profile_values:
                (profile_path, env_vars) = profile_values
                env.update(env_vars)
                logger.debug("Using environment variables keys: %s", env.keys())

                flags = self._generate_dbt_flags(tmp_project_dir, profile_path)

                if self.install_deps:
                    self._install_dependencies(
                        tmp_dir_path, flags + self._process_global_flag("--vars", self.vars), env
                    )

                if run_as_async and not settings.enable_setup_async_task:
                    self._mock_dbt_adapter(async_context)

                full_cmd = cmd + flags
                result = self.invoke_dbt(
                    command=full_cmd,
                    env=env,
                    cwd=tmp_project_dir,
                )
                if is_openlineage_common_available:
                    self.calculate_openlineage_events_completes(env, tmp_dir_path)
                    if AIRFLOW_VERSION.major < _AIRFLOW3_MAJOR_VERSION:
                        # Airflow 3 does not support associating 'openlineage_events_completes' with task_instance,
                        # in that case we're storing as self.openlineage_events_completes
                        context["task_instance"].openlineage_events_completes = self.openlineage_events_completes  # type: ignore[attr-defined]

                if self.emit_datasets:
                    self._handle_datasets(context)

                if self.partial_parse:
                    self._update_partial_parse_cache(tmp_dir_path)

                self._handle_post_execution(tmp_project_dir, context, push_run_results_to_xcom)
                self.handle_exception(result)

                if run_as_async and async_context:
                    self._handle_async_execution(tmp_project_dir, context, async_context)

                return result

    def calculate_openlineage_events_completes(
        self, env: dict[str, str | os.PathLike[Any] | bytes], project_dir: Path
    ) -> None:
        """
        Use openlineage-integration-common to extract lineage events from the artifacts generated after running the dbt
        command. Relies on the following files:
        * profiles
        * {project_dir}/target/manifest.json
        * {project_dir}/target/run_results.json

        Return a list of RunEvents
        """
        # Since openlineage-integration-common relies on the profiles definition, we need to make these newly introduced
        # environment variables to the library. As of 1.0.0, DbtLocalArtifactProcessor did not allow passing environment
        # variables as an argument, so we need to inject them to the system environment variables.
        for key, value in env.items():
            os.environ[key] = str(value)

        openlineage_processor = DbtLocalArtifactProcessor(
            producer=OPENLINEAGE_PRODUCER,
            job_namespace=settings.LINEAGE_NAMESPACE,
            project_dir=project_dir,
            profile_name=self.profile_config.profile_name,
            target=self.profile_config.target_name,
        )
        # Do not raise exception if a command is unsupported, following the openlineage-dbt processor:
        # https://github.com/OpenLineage/OpenLineage/blob/bdcaf828ebc117e0e5ffc5fab44ff8886eb7836b/integration/common/openlineage/common/provider/dbt/processor.py#L141
        openlineage_processor.should_raise_on_unsupported_command = False
        try:
            events = openlineage_processor.parse()
            self.openlineage_events_completes = events.completes
        except (FileNotFoundError, NotImplementedError, ValueError, KeyError, jinja2.exceptions.UndefinedError):
            logger.debug("Unable to parse OpenLineage events", stack_info=True)

    @staticmethod
    def _create_asset_uri(openlineage_event: openlineage.client.generated.base.OutputDataset) -> str:
        """
        Create the Airflow Asset or Dataset UIR given an OpenLineage event.
        """
        airflow_2_uri = str(openlineage_event.namespace + "/" + urllib.parse.quote(openlineage_event.name))
        airflow_3_uri = str(
            openlineage_event.namespace + "/" + urllib.parse.quote(openlineage_event.name).replace(".", "/")
        )
        if AIRFLOW_VERSION < Version("3.0.0"):
            if settings.use_dataset_airflow3_uri_standard:
                dataset_uri = airflow_3_uri
            else:
                logger.warning(
                    f"""
                    Airflow 3.0.0 Asset (Dataset) URIs validation rules changed and OpenLineage URIs (standard used by Cosmos) will no longer be valid.
                    Therefore, if using Cosmos with Airflow 3, the Airflow Dataset URIs will be changed to <{airflow_3_uri}>.
                    Previously, with Airflow 2.x, the URI was <{airflow_2_uri}>.
                    If you want to use the Airflow 3 URI standard while still using Airflow 2, please, set:
                        export AIRFLOW__COSMOS__USE_DATASET_AIRFLOW3_URI_STANDARD=1
                    Remember to update any DAGs that are scheduled using this dataset.
                    """
                )
                dataset_uri = airflow_2_uri
        else:
            logger.warning(
                f"""
                Airflow 3.0.0 Asset (Dataset) URIs validation rules changed and OpenLineage URIs (standard used by Cosmos) are no longer accepted.
                Therefore, if using Cosmos with Airflow 3, the Airflow Asset (Dataset) URI is now <{airflow_3_uri}>.
                Before, with Airflow 2.x, the URI used to be <{airflow_2_uri}>.
                Please, change any DAGs that were scheduled using the old standard to the new one.
                """
            )
            dataset_uri = airflow_3_uri
        return dataset_uri

    def get_datasets(self, source: Literal["inputs", "outputs"]) -> list[Asset]:
        """
        Use openlineage-integration-common to extract lineage events from the artifacts generated after running the dbt
        command. Relies on the following files:
        * profiles
        * {project_dir}/target/manifest.json
        * {project_dir}/target/run_results.json

        Return a list of Dataset URIs (strings).
        """
        uris = []

        for completed in self.openlineage_events_completes:
            for output in getattr(completed, source):
                dataset_uri = self._create_asset_uri(output)
                uris.append(dataset_uri)
        logger.debug("URIs to be converted to Asset: %s", uris)

        assets = [Asset(uri) for uri in uris]

        return assets

    def register_dataset(self, new_inlets: list[Asset], new_outlets: list[Asset], context: Context) -> None:
        """
        Register a list of datasets as outlets of the current task, when possible.

        Until Airflow 2.7, there was not a better interface to associate outlets to a task during execution.
        This works in Cosmos with versions before Airflow 2.10 with a few limitations, as described in the ticket:
        https://github.com/astronomer/astronomer-cosmos/issues/522

        Since Airflow 2.10, Cosmos uses DatasetAlias by default, to generate datasets. This resolved the limitations
        described before.

        The only limitation is that with Airflow 2.10.0 and 2.10.1, the `airflow dags test` command will not work
        with DatasetAlias:
        https://github.com/apache/airflow/issues/42495
        """
        if AIRFLOW_VERSION.major >= 3 and not settings.enable_dataset_alias:
            logger.error("To emit datasets with Airflow 3, the setting `enable_dataset_alias` must be True (default).")
            raise AirflowCompatibilityError(
                "To emit datasets with Airflow 3, the setting `enable_dataset_alias` must be True (default)."
            )
        elif AIRFLOW_VERSION < Version("2.10") or not settings.enable_dataset_alias:
            from airflow.utils.session import create_session

            logger.info("Assigning inlets/outlets without DatasetAlias")
            with create_session() as session:
                self.outlets.extend(new_outlets)  # type: ignore[attr-defined]
                self.inlets.extend(new_inlets)  # type: ignore[attr-defined]
                for task in self.dag.tasks:  # type: ignore[attr-defined]
                    if task.task_id == self.task_id:
                        task.outlets.extend(new_outlets)
                        task.inlets.extend(new_inlets)
                DAG.bulk_write_to_db([self.dag], session=session)  # type: ignore[attr-defined, call-arg, arg-type]
                session.commit()
        else:
            dataset_alias_name = get_dataset_alias_name(self.dag, self.task_group, self.task_id)  # type: ignore[attr-defined]

            if AIRFLOW_VERSION.major == 2:
                logger.info("Assigning inlets/outlets with DatasetAlias in Airflow 2")

                for outlet in new_outlets:
                    context["outlet_events"][dataset_alias_name].add(outlet)  # type: ignore[index]
            else:  # AIRFLOW_VERSION.major == 3
                logger.info("Assigning outlets with DatasetAlias in Airflow 3")
                from airflow.sdk.definitions.asset import AssetAlias

                # This line was necessary in Airflow 3.0.0, but this may become automatic in newer versions
                self.outlets.append(AssetAlias(dataset_alias_name))  # type: ignore[attr-defined, call-arg, arg-type]
                for outlet in new_outlets:
                    context["outlet_events"][AssetAlias(dataset_alias_name)].add(outlet)

    def get_openlineage_facets_on_complete(self, task_instance: TaskInstance) -> OperatorLineage:
        """
        Collect the input, output, job and run facets for this operator.
        It relies on the calculate_openlineage_events_completes having being called before.

        This method is called by Openlineage even if `execute` fails, because `get_openlineage_facets_on_failure`
        is not implemented.
        """

        inputs = []
        outputs = []
        run_facets: dict[str, Any] = {}
        job_facets: dict[str, Any] = {}

        openlineage_events_completes = None
        if hasattr(self, "openlineage_events_completes"):
            openlineage_events_completes = self.openlineage_events_completes
        elif hasattr(task_instance, "openlineage_events_completes"):
            openlineage_events_completes = task_instance.openlineage_events_completes
        else:
            logger.info("Unable to emit OpenLineage events due to lack of data.")

        if openlineage_events_completes is not None:
            for completed in openlineage_events_completes:
                [inputs.append(input_) for input_ in completed.inputs if input_ not in inputs]  # type: ignore
                [outputs.append(output) for output in completed.outputs if output not in outputs]  # type: ignore
                run_facets = {**run_facets, **completed.run.facets}
                job_facets = {**job_facets, **completed.job.facets}
        else:
            logger.info("Unable to emit OpenLineage events due to lack of dependencies or data.")

        return OperatorLineage(
            inputs=inputs,
            outputs=outputs,
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> FullOutputSubprocessResult | dbtRunnerResult:
        # If this is an async run and we're using the setup task, make sure to include the full_refresh flag if set
        if run_as_async and settings.enable_setup_async_task and getattr(self, "full_refresh", False):
            if cmd_flags is None:
                cmd_flags = []
            if "--full-refresh" not in cmd_flags:
                cmd_flags.append("--full-refresh")

        dbt_cmd, env = self.build_cmd(context=context, cmd_flags=cmd_flags)
        dbt_cmd = dbt_cmd or []
        result = self.run_command(
            cmd=dbt_cmd,
            env=env,
            context=context,
            run_as_async=run_as_async,
            async_context=async_context,
            push_run_results_to_xcom=kwargs.get("push_run_results_to_xcom", False),
        )
        return result

    def on_kill(self) -> None:
        if self.invocation_mode == InvocationMode.SUBPROCESS:
            if self.cancel_query_on_kill:
                self.subprocess_hook.send_sigint()
            else:
                self.subprocess_hook.send_sigterm()


class DbtLocalBaseOperator(AbstractDbtLocalBase, BaseOperator):  # type: ignore[misc]
    template_fields: Sequence[str] = AbstractDbtLocalBase.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
        # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
        # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
        # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
        # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
        # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
        base_kwargs = {}
        operator_kwargs = {}
        operator_args = {*inspect.signature(BaseOperator.__init__).parameters.keys()}

        default_args = kwargs.get("default_args", {})

        for arg in operator_args:
            try:
                operator_kwargs[arg] = kwargs[arg]
            except KeyError:
                pass

        for arg in {
            *inspect.getfullargspec(AbstractDbtBase.__init__).args,
            *inspect.getfullargspec(AbstractDbtLocalBase.__init__).args,
        }:
            try:
                base_kwargs[arg] = kwargs[arg]
            except KeyError:
                try:
                    base_kwargs[arg] = default_args[arg]
                except KeyError:
                    pass

        AbstractDbtLocalBase.__init__(self, **base_kwargs)
        if AIRFLOW_VERSION.major < _AIRFLOW3_MAJOR_VERSION:
            if (
                kwargs.get("emit_datasets", True)
                and settings.enable_dataset_alias
                and AIRFLOW_VERSION >= Version("2.10")
            ):
                from airflow.datasets import DatasetAlias

                # ignoring the type because older versions of Airflow raise the follow error in mypy
                # error: Incompatible types in assignment (expression has type "list[DatasetAlias]", target has type "str")
                dag_id = kwargs.get("dag")
                task_group_id = kwargs.get("task_group")
                operator_kwargs["outlets"] = [
                    DatasetAlias(name=get_dataset_alias_name(dag_id, task_group_id, self.task_id))
                ]  # type: ignore

        if "task_id" in operator_kwargs:
            operator_kwargs.pop("task_id")
        BaseOperator.__init__(self, task_id=self.task_id, **operator_kwargs)


class DbtBuildLocalOperator(DbtBuildMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.on_warning_callback = on_warning_callback
        self.extract_issues: Callable[..., tuple[list[str], list[str]]]

    def _handle_warnings(self, result: FullOutputSubprocessResult | dbtRunnerResult, context: Context) -> None:
        """
         Handles warnings by extracting log issues, creating additional context, and calling the
         on_warning_callback with the updated context.

        :param result: The result object from the build and run command.
        :param context: The original airflow context in which the build and run command was executed.
        """
        if self.invocation_mode == InvocationMode.SUBPROCESS:
            self.extract_issues = extract_freshness_warn_msg
        elif self.invocation_mode == InvocationMode.DBT_RUNNER:
            self.extract_issues = dbt_runner.extract_message_by_status

        test_names, test_results = self.extract_issues(result)

        warning_context = dict(context)
        warning_context["test_names"] = test_names
        warning_context["test_results"] = test_results

        self.on_warning_callback and self.on_warning_callback(warning_context)

    def execute(self, context: Context, **kwargs: Any) -> None:
        result = self.build_and_run_cmd(context=context, cmd_flags=self.add_cmd_flags(), **kwargs)
        if self.on_warning_callback:
            self._handle_warnings(result, context)


class DbtLSLocalOperator(DbtLSMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core ls command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedLocalOperator(DbtSeedMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core seed command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotLocalOperator(DbtSnapshotMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core snapshot command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceLocalOperator(DbtSourceMixin, DbtLocalBaseOperator):
    """
    Executes a dbt source freshness command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.on_warning_callback = on_warning_callback
        self.extract_issues: Callable[..., tuple[list[str], list[str]]]

    def _handle_warnings(self, result: FullOutputSubprocessResult | dbtRunnerResult, context: Context) -> None:
        """
         Handles warnings by extracting log issues, creating additional context, and calling the
         on_warning_callback with the updated context.

        :param result: The result object from the build and run command.
        :param context: The original airflow context in which the build and run command was executed.
        """
        if self.invocation_mode == InvocationMode.SUBPROCESS:
            self.extract_issues = extract_freshness_warn_msg
        elif self.invocation_mode == InvocationMode.DBT_RUNNER:
            self.extract_issues = dbt_runner.extract_message_by_status

        test_names, test_results = self.extract_issues(result)

        warning_context = dict(context)
        warning_context["test_names"] = test_names
        warning_context["test_results"] = test_results

        self.on_warning_callback and self.on_warning_callback(warning_context)

    def execute(self, context: Context, **kwargs: Any) -> None:
        result = self.build_and_run_cmd(context=context, cmd_flags=self.add_cmd_flags())
        if self.on_warning_callback:
            self._handle_warnings(result, context)


class DbtRunLocalOperator(DbtRunMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestLocalOperator(DbtTestMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core test command.
    :param on_warning_callback: A callback function called on warnings with additional Context variables "test_names"
        and "test_results" of type `List`. Each index in "test_names" corresponds to the same index in "test_results".
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields  # type: ignore[operator]

    def __init__(
        self,
        on_warning_callback: Callable[..., Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.on_warning_callback = on_warning_callback
        self.extract_issues: Callable[..., tuple[list[str], list[str]]]
        self.parse_number_of_warnings: Callable[..., int]

    def _handle_warnings(self, result: FullOutputSubprocessResult | dbtRunnerResult, context: Context) -> None:
        """
         Handles warnings by extracting log issues, creating additional context, and calling the
         on_warning_callback with the updated context.

        :param result: The result object from the build and run command.
        :param context: The original airflow context in which the build and run command was executed.
        """
        test_names, test_results = self.extract_issues(result)

        warning_context = dict(context)
        warning_context["test_names"] = test_names
        warning_context["test_results"] = test_results

        self.on_warning_callback and self.on_warning_callback(warning_context)

    def _set_test_result_parsing_methods(self) -> None:
        """Sets the extract_issues and parse_number_of_warnings methods based on the invocation mode."""
        if self.invocation_mode == InvocationMode.SUBPROCESS:
            self.extract_issues = lambda result: extract_log_issues(result.full_output)
            self.parse_number_of_warnings = parse_number_of_warnings_subprocess
        elif self.invocation_mode == InvocationMode.DBT_RUNNER:
            self.extract_issues = dbt_runner.extract_message_by_status
            self.parse_number_of_warnings = dbt_runner.parse_number_of_warnings

    def execute(self, context: Context, **kwargs: Any) -> None:
        result = self.build_and_run_cmd(context=context, cmd_flags=self.add_cmd_flags())
        self._set_test_result_parsing_methods()
        number_of_warnings = self.parse_number_of_warnings(result)  # type: ignore
        if self.on_warning_callback and number_of_warnings > 0:
            self._handle_warnings(result, context)


class DbtRunOperationLocalOperator(DbtRunOperationMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtDocsLocalOperator(DbtLocalBaseOperator):
    """
    Executes `dbt docs generate` command.
    Use the `callback` parameter to specify a callback function to run after the command completes.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields  # type: ignore[operator]

    ui_color = "#8194E0"
    required_files = ["index.html", "manifest.json", "catalog.json"]
    base_cmd = ["docs", "generate"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.check_static_flag()

    def check_static_flag(self) -> None:
        if self.dbt_cmd_flags:
            if "--static" in self.dbt_cmd_flags:
                # For the --static flag we only upload the generated static_index.html file
                self.required_files = ["static_index.html"]
        if self.dbt_cmd_global_flags:
            if "--no-write-json" in self.dbt_cmd_global_flags and "graph.gpickle" in self.required_files:
                self.required_files.remove("graph.gpickle")


class DbtDocsCloudLocalOperator(DbtDocsLocalOperator, ABC):
    """
    Abstract class for operators that upload the generated documentation to cloud storage.
    """

    template_fields: Sequence[str] = DbtDocsLocalOperator.template_fields  # type: ignore[operator]

    def __init__(
        self,
        connection_id: str,
        bucket_name: str,
        folder_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initializes the operator."""
        self.connection_id = connection_id
        self.bucket_name = bucket_name
        self.folder_dir = folder_dir

        super().__init__(**kwargs)

        # override the callback with our own
        self.callback = self.upload_to_cloud_storage

    @abstractmethod
    def upload_to_cloud_storage(self, project_dir: str, **kwargs: Any) -> None:
        """Abstract method to upload the generated documentation to cloud storage."""


class DbtDocsS3LocalOperator(DbtDocsCloudLocalOperator):
    """
    Executes `dbt docs generate` command and upload to S3 storage.

    :param connection_id: S3's Airflow connection ID
    :param bucket_name: S3's bucket name
    :param folder_dir: This can be used to specify under which directory the generated DBT documentation should be
        uploaded.
    """

    ui_color = "#FF9900"

    def __init__(
        self,
        *args: Any,
        aws_conn_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        if aws_conn_id:
            warnings.warn(
                "Please, use `connection_id` instead of `aws_conn_id`. The argument `aws_conn_id` will be"
                " deprecated in Cosmos 2.0",
                DeprecationWarning,
            )
            kwargs["connection_id"] = aws_conn_id
        super().__init__(*args, **kwargs)

    def upload_to_cloud_storage(self, project_dir: str, **kwargs: Any) -> None:
        """Uploads the generated documentation to S3."""
        logger.info(
            'Attempting to upload generated docs to S3 using S3Hook("%s")',
            self.connection_id,
        )

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        target_dir = f"{project_dir}/target"

        hook = S3Hook(
            self.connection_id,
            extra_args={
                "ContentType": "text/html",
            },
        )

        for filename in self.required_files:
            key = f"{self.folder_dir}/{filename}" if self.folder_dir else filename
            s3_path = f"s3://{self.bucket_name}/{key}"
            logger.info("Uploading %s to %s", filename, s3_path)

            hook.load_file(
                filename=f"{target_dir}/{filename}",
                bucket_name=self.bucket_name,
                key=key,
                replace=True,
            )


class DbtDocsAzureStorageLocalOperator(DbtDocsCloudLocalOperator):
    """
    Executes `dbt docs generate` command and upload to Azure Blob Storage.

    :param connection_id: Azure Blob Storage's Airflow connection ID
    :param bucket_name: Azure Blob Storage's bucket name
    :param folder_dir: This can be used to specify under which directory the generated DBT documentation should be
        uploaded.
    """

    ui_color = "#007FFF"

    def __init__(
        self,
        *args: Any,
        azure_conn_id: str | None = None,
        container_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        if azure_conn_id:
            warnings.warn(
                "Please, use `connection_id` instead of `azure_conn_id`. The argument `azure_conn_id` will"
                " be deprecated in Cosmos 2.0",
                DeprecationWarning,
            )
            kwargs["connection_id"] = azure_conn_id
        if container_name:
            warnings.warn(
                "Please, use `bucket_name` instead of `container_name`. The argument `container_name` will"
                " be deprecated in Cosmos 2.0",
                DeprecationWarning,
            )
            kwargs["bucket_name"] = container_name
        super().__init__(*args, **kwargs)

    def upload_to_cloud_storage(self, project_dir: str, **kwargs: Any) -> None:
        """Uploads the generated documentation to Azure Blob Storage."""
        logger.info(
            'Attempting to upload generated docs to Azure Blob Storage using WasbHook(conn_id="%s")',
            self.connection_id,
        )

        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        target_dir = f"{project_dir}/target"

        hook = WasbHook(
            self.connection_id,
        )

        for filename in self.required_files:
            logger.info(
                "Uploading %s to %s",
                filename,
                f"wasb://{self.bucket_name}/{filename}",
            )

            blob_name = f"{self.folder_dir}/{filename}" if self.folder_dir else filename

            hook.load_file(
                file_path=f"{target_dir}/{filename}",
                container_name=self.bucket_name,
                blob_name=blob_name,
                overwrite=True,
            )


class DbtDocsGCSLocalOperator(DbtDocsCloudLocalOperator):
    """
    Executes `dbt docs generate` command and upload to GCS.

    :param connection_id: Google Cloud Storage's Airflow connection ID
    :param bucket_name: Google Cloud Storage's bucket name
    :param folder_dir: This can be used to specify under which directory the generated DBT documentation should be
        uploaded.
    """

    ui_color = "#4772d5"

    def upload_to_cloud_storage(self, project_dir: str, **kwargs: Any) -> None:
        """Uploads the generated documentation to Google Cloud Storage"""
        logger.info(
            'Attempting to upload generated docs to Storage using GCSHook(conn_id="%s")',
            self.connection_id,
        )

        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        target_dir = f"{project_dir}/target"
        hook = GCSHook(self.connection_id)

        for filename in self.required_files:
            blob_name = f"{self.folder_dir}/{filename}" if self.folder_dir else filename
            logger.info("Uploading %s to %s", filename, f"gs://{self.bucket_name}/{blob_name}")
            hook.upload(
                filename=f"{target_dir}/{filename}",
                bucket_name=self.bucket_name,
                object_name=blob_name,
            )


class DbtDepsLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core deps command.
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs: str) -> None:
        raise DeprecationWarning(
            "The DbtDepsOperator has been deprecated. Please use the `install_deps` flag in dbt_args instead."
        )


class DbtCompileLocalOperator(DbtCompileMixin, DbtLocalBaseOperator):
    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["should_upload_compiled_sql"] = True
        super().__init__(*args, **kwargs)


class DbtCloneLocalOperator(DbtCloneMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core clone command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
