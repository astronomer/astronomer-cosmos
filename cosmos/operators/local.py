from __future__ import annotations

import os
import tempfile
import warnings
from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal, Sequence

import jinja2
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from attr import define

from cosmos import cache
from cosmos.constants import InvocationMode
from cosmos.dbt.project import get_partial_parse_path, has_non_empty_dependencies_file
from cosmos.exceptions import AirflowCompatibilityError
from cosmos.settings import LINEAGE_NAMESPACE

try:
    from airflow.datasets import Dataset
    from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor
except ModuleNotFoundError:
    is_openlineage_available = False
    DbtLocalArtifactProcessor = None
else:
    is_openlineage_available = True

if TYPE_CHECKING:
    from airflow.datasets import Dataset  # noqa: F811
    from dbt.cli.main import dbtRunner, dbtRunnerResult
    from openlineage.client.run import RunEvent

from sqlalchemy.orm import Session

from cosmos.config import ProfileConfig
from cosmos.constants import (
    OPENLINEAGE_PRODUCER,
)
from cosmos.dbt.parser.output import (
    extract_dbt_runner_issues,
    extract_log_issues,
    parse_number_of_warnings_dbt_runner,
    parse_number_of_warnings_subprocess,
)
from cosmos.dbt.project import change_working_directory, create_symlinks, environ
from cosmos.hooks.subprocess import (
    FullOutputSubprocessHook,
    FullOutputSubprocessResult,
)
from cosmos.log import get_logger
from cosmos.operators.base import (
    AbstractDbtBaseOperator,
    DbtBuildMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtTestMixin,
)

logger = get_logger(__name__)

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
            "Further details on lack of Openlineage:",
            stack_info=True,
        )
        is_openlineage_available = False

        @define
        class OperatorLineage:  # type: ignore
            inputs: list[str] = list()
            outputs: list[str] = list()
            run_facets: dict[str, str] = dict()
            job_facets: dict[str, str] = dict()


class DbtLocalBaseOperator(AbstractDbtBaseOperator):
    """
    Executes a dbt core cli command locally.

    :param profile_args: Arguments to pass to the profile. See
        :py:class:`cosmos.providers.dbt.core.profiles.BaseProfileMapping`.
    :param profile_name: A name to use for the dbt profile. If not provided, and no profile target is found
        in your project's dbt_project.yml, "cosmos_profile" is used.
    :param install_deps: If true, install dependencies before running the command
    :param callback: A callback function called on after a dbt run with a path to the dbt project directory.
    :param target_name: A name to use for the dbt target. If not provided, and no target is found
        in your project's dbt_project.yml, "cosmos_target" is used.
    :param should_store_compiled_sql: If true, store the compiled SQL in the compiled_sql rendered template.
    :param append_env: If True(default), inherits the environment variables
        from current process and then environment variable passed by the user will either update the existing
        inherited environment variables or the new variables gets appended to it.
        If False, only uses the environment variables passed in env params
        and does not inherit the current process environment.
    """

    template_fields: Sequence[str] = AbstractDbtBaseOperator.template_fields + ("compiled_sql",)  # type: ignore[operator]
    template_fields_renderers = {
        "compiled_sql": "sql",
    }

    def __init__(
        self,
        profile_config: ProfileConfig,
        invocation_mode: InvocationMode | None = None,
        install_deps: bool = False,
        callback: Callable[[str], None] | None = None,
        should_store_compiled_sql: bool = True,
        append_env: bool = True,
        **kwargs: Any,
    ) -> None:
        self.profile_config = profile_config
        self.callback = callback
        self.compiled_sql = ""
        self.should_store_compiled_sql = should_store_compiled_sql
        self.openlineage_events_completes: list[RunEvent] = []
        self.invocation_mode = invocation_mode
        self.invoke_dbt: Callable[..., FullOutputSubprocessResult | dbtRunnerResult]
        self.handle_exception: Callable[..., None]
        self._dbt_runner: dbtRunner | None = None
        if self.invocation_mode:
            self._set_invocation_methods()
        kwargs.pop("full_refresh", None)  # usage of this param should be implemented in child classes
        super().__init__(**kwargs)

        # For local execution mode, we're consistent with the LoadMode.DBT_LS command in forwarding the environment
        # variables to the subprocess by default. Although this behavior is designed for ExecuteMode.LOCAL and
        # ExecuteMode.VIRTUALENV, it is not desired for the other execution modes to forward the environment variables
        # as it can break existing DAGs.
        self.append_env = append_env

        # We should not spend time trying to install deps if the project doesn't have any dependencies
        self.install_deps = install_deps and has_non_empty_dependencies_file(Path(self.project_dir))

    @cached_property
    def subprocess_hook(self) -> FullOutputSubprocessHook:
        """Returns hook for running the bash command."""
        return FullOutputSubprocessHook()

    def _set_invocation_methods(self) -> None:
        """Sets the associated run and exception handling methods based on the invocation mode."""
        if self.invocation_mode == InvocationMode.SUBPROCESS:
            self.invoke_dbt = self.run_subprocess
            self.handle_exception = self.handle_exception_subprocess
        elif self.invocation_mode == InvocationMode.DBT_RUNNER:
            self.invoke_dbt = self.run_dbt_runner
            self.handle_exception = self.handle_exception_dbt_runner

    def _discover_invocation_mode(self) -> None:
        """Discovers the invocation mode based on the availability of dbtRunner for import. If dbtRunner is available, it will
        be used since it is faster than subprocess. If dbtRunner is not available, it will fall back to subprocess.
        This method is called at runtime to work in the environment where the operator is running.
        """
        try:
            from dbt.cli.main import dbtRunner  # noqa
        except ImportError:
            self.invocation_mode = InvocationMode.SUBPROCESS
            logger.info("Could not import dbtRunner. Falling back to subprocess for invoking dbt.")
        else:
            self.invocation_mode = InvocationMode.DBT_RUNNER
            logger.info("dbtRunner is available. Using dbtRunner for invoking dbt.")
        self._set_invocation_methods()

    def handle_exception_subprocess(self, result: FullOutputSubprocessResult) -> None:
        if self.skip_exit_code is not None and result.exit_code == self.skip_exit_code:
            raise AirflowSkipException(f"dbt command returned exit code {self.skip_exit_code}. Skipping.")
        elif result.exit_code != 0:
            logger.error("\n".join(result.full_output))
            raise AirflowException(f"dbt command failed. The command returned a non-zero exit code {result.exit_code}.")

    def handle_exception_dbt_runner(self, result: dbtRunnerResult) -> None:
        """dbtRunnerResult has an attribute `success` that is False if the command failed."""
        if not result.success:
            if result.exception:
                raise AirflowException(f"dbt invocation did not complete with unhandled error: {result.exception}")
            else:
                node_names, node_results = extract_dbt_runner_issues(result, ["error", "fail", "runtime error"])
                error_message = "\n".join([f"{name}: {result}" for name, result in zip(node_names, node_results)])
                raise AirflowException(f"dbt invocation completed with errors: {error_message}")

    @provide_session
    def store_compiled_sql(self, tmp_project_dir: str, context: Context, session: Session = NEW_SESSION) -> None:
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

        # need to refresh the rendered task field record in the db because Airflow only does this
        # before executing the task, not after
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        ti = context["ti"]

        if isinstance(ti, TaskInstance):  # verifies ti is a TaskInstance in order to access and use the "task" field
            if TYPE_CHECKING:
                assert ti.task is not None
            ti.task.template_fields = self.template_fields
            rtif = RenderedTaskInstanceFields(ti, render_templates=False)

            # delete the old records
            session.query(RenderedTaskInstanceFields).filter(
                RenderedTaskInstanceFields.dag_id == self.dag_id,
                RenderedTaskInstanceFields.task_id == self.task_id,
                RenderedTaskInstanceFields.run_id == ti.run_id,
            ).delete()
            session.add(rtif)
        else:
            logger.info("Warning: ti is of type TaskInstancePydantic. Cannot update template_fields.")

    def run_subprocess(self, command: list[str], env: dict[str, str], cwd: str) -> FullOutputSubprocessResult:
        logger.info("Trying to run the command:\n %s\nFrom %s", command, cwd)
        subprocess_result: FullOutputSubprocessResult = self.subprocess_hook.run_command(
            command=command,
            env=env,
            cwd=cwd,
            output_encoding=self.output_encoding,
        )
        logger.info(subprocess_result.output)
        return subprocess_result

    def run_dbt_runner(self, command: list[str], env: dict[str, str], cwd: str) -> dbtRunnerResult:
        """Invokes the dbt command programmatically."""
        try:
            from dbt.cli.main import dbtRunner
        except ImportError:
            raise ImportError(
                "Could not import dbt core. Ensure that dbt-core >= v1.5 is installed and available in the environment where the operator is running."
            )

        if self._dbt_runner is None:
            self._dbt_runner = dbtRunner()

        # Exclude the dbt executable path from the command
        cli_args = command[1:]
        logger.info("Trying to run dbtRunner with:\n %s\n in %s", cli_args, cwd)

        with change_working_directory(cwd), environ(env):
            result = self._dbt_runner.invoke(cli_args)

        return result

    def run_command(
        self,
        cmd: list[str],
        env: dict[str, str | bytes | os.PathLike[Any]],
        context: Context,
    ) -> FullOutputSubprocessResult | dbtRunnerResult:
        """
        Copies the dbt project to a temporary directory and runs the command.
        """
        if not self.invocation_mode:
            self._discover_invocation_mode()

        with tempfile.TemporaryDirectory() as tmp_project_dir:

            logger.info(
                "Cloning project to writable temp directory %s from %s",
                tmp_project_dir,
                self.project_dir,
            )
            tmp_dir_path = Path(tmp_project_dir)
            env = {k: str(v) for k, v in env.items()}
            create_symlinks(Path(self.project_dir), tmp_dir_path, self.install_deps)

            if self.partial_parse and self.cache_dir is not None:
                latest_partial_parse = cache._get_latest_partial_parse(Path(self.project_dir), self.cache_dir)
                logger.info("Partial parse is enabled and the latest partial parse file is %s", latest_partial_parse)
                if latest_partial_parse is not None:
                    cache._copy_partial_parse_to_project(latest_partial_parse, tmp_dir_path)

            with self.profile_config.ensure_profile() as profile_values:
                (profile_path, env_vars) = profile_values
                env.update(env_vars)

                flags = [
                    "--project-dir",
                    str(tmp_project_dir),
                    "--profiles-dir",
                    str(profile_path.parent),
                    "--profile",
                    self.profile_config.profile_name,
                    "--target",
                    self.profile_config.target_name,
                ]

                if self.install_deps:
                    deps_command = [self.dbt_executable_path, "deps"]
                    deps_command.extend(flags)
                    self.invoke_dbt(
                        command=deps_command,
                        env=env,
                        cwd=tmp_project_dir,
                    )

                full_cmd = cmd + flags

                logger.debug("Using environment variables keys: %s", env.keys())

                result = self.invoke_dbt(
                    command=full_cmd,
                    env=env,
                    cwd=tmp_project_dir,
                )
                if is_openlineage_available:
                    self.calculate_openlineage_events_completes(env, tmp_dir_path)
                    context[
                        "task_instance"
                    ].openlineage_events_completes = self.openlineage_events_completes  # type: ignore

                if self.emit_datasets:
                    inlets = self.get_datasets("inputs")
                    outlets = self.get_datasets("outputs")
                    logger.info("Inlets: %s", inlets)
                    logger.info("Outlets: %s", outlets)
                    self.register_dataset(inlets, outlets)

                if self.partial_parse and self.cache_dir:
                    partial_parse_file = get_partial_parse_path(tmp_dir_path)
                    if partial_parse_file.exists():
                        cache._update_partial_parse_cache(partial_parse_file, self.cache_dir)

                self.store_compiled_sql(tmp_project_dir, context)
                self.handle_exception(result)
                if self.callback:
                    self.callback(tmp_project_dir)

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
            job_namespace=LINEAGE_NAMESPACE,
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

    def get_datasets(self, source: Literal["inputs", "outputs"]) -> list[Dataset]:
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
                dataset_uri = output.namespace + "/" + output.name
                uris.append(dataset_uri)
        logger.debug("URIs to be converted to Dataset: %s", uris)

        datasets = []
        try:
            datasets = [Dataset(uri) for uri in uris]
        except ValueError:
            raise AirflowCompatibilityError(
                """
                Apache Airflow 2.9.0 & 2.9.1 introduced a breaking change in Dataset URIs, to be fixed in newer versions:
                https://github.com/apache/airflow/issues/39486

                If you want to use Cosmos with one of these Airflow versions, you will have to disable emission of Datasets:
                By setting ``emit_datasets=False`` in ``RenderConfig``. For more information, see https://astronomer.github.io/astronomer-cosmos/configuration/render-config.html.
                """
            )
        return datasets

    def register_dataset(self, new_inlets: list[Dataset], new_outlets: list[Dataset]) -> None:
        """
        Register a list of datasets as outlets of the current task.
        Until Airflow 2.7, there was not a better interface to associate outlets to a task during execution.
        """
        with create_session() as session:
            self.outlets.extend(new_outlets)
            self.inlets.extend(new_inlets)
            for task in self.dag.tasks:
                if task.task_id == self.task_id:
                    task.outlets.extend(new_outlets)
                    task.inlets.extend(new_inlets)
            DAG.bulk_write_to_db([self.dag], session=session)
            session.commit()

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
        self, context: Context, cmd_flags: list[str] | None = None
    ) -> FullOutputSubprocessResult | dbtRunnerResult:
        dbt_cmd, env = self.build_cmd(context=context, cmd_flags=cmd_flags)
        dbt_cmd = dbt_cmd or []
        result = self.run_command(cmd=dbt_cmd, env=env, context=context)
        return result

    def on_kill(self) -> None:
        if self.invocation_mode == InvocationMode.SUBPROCESS:
            if self.cancel_query_on_kill:
                self.subprocess_hook.send_sigint()
            else:
                self.subprocess_hook.send_sigterm()


class DbtBuildLocalOperator(DbtBuildMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]


class DbtLSLocalOperator(DbtLSMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core ls command.
    """


class DbtSeedLocalOperator(DbtSeedMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core seed command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]


class DbtSnapshotLocalOperator(DbtSnapshotMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core snapshot command.
    """


class DbtRunLocalOperator(DbtRunMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtLocalBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]


class DbtTestLocalOperator(DbtTestMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core test command.
    :param on_warning_callback: A callback function called on warnings with additional Context variables "test_names"
        and "test_results" of type `List`. Each index in "test_names" corresponds to the same index in "test_results".
    """

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
            self.extract_issues = extract_dbt_runner_issues
            self.parse_number_of_warnings = parse_number_of_warnings_dbt_runner

    def execute(self, context: Context) -> None:
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


class DbtDocsLocalOperator(DbtLocalBaseOperator):
    """
    Executes `dbt docs generate` command.
    Use the `callback` parameter to specify a callback function to run after the command completes.
    """

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

    def __init__(
        self,
        connection_id: str,
        bucket_name: str,
        folder_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        "Initializes the operator."
        self.connection_id = connection_id
        self.bucket_name = bucket_name
        self.folder_dir = folder_dir

        super().__init__(**kwargs)

        # override the callback with our own
        self.callback = self.upload_to_cloud_storage

    @abstractmethod
    def upload_to_cloud_storage(self, project_dir: str) -> None:
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

    def upload_to_cloud_storage(self, project_dir: str) -> None:
        "Uploads the generated documentation to S3."
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

    def upload_to_cloud_storage(self, project_dir: str) -> None:
        "Uploads the generated documentation to Azure Blob Storage."
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

    def upload_to_cloud_storage(self, project_dir: str) -> None:
        "Uploads the generated documentation to Google Cloud Storage"
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
            "The DbtDepsOperator has been deprecated. " "Please use the `install_deps` flag in dbt_args instead."
        )
