from __future__ import annotations

import inspect
import time
from abc import ABC
from typing import Any, Callable, Sequence

from airflow.models import TaskInstance
from airflow.utils.context import Context

from cosmos.config import ProfileConfig
from cosmos.dbt.parser.output import extract_log_issues
from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtCloneMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
)

DBT_NO_TESTS_MSG = "Nothing to do"
DBT_WARN_MSG = "WARN"

DEFAULT_ENVIRONMENT_VARIABLES: dict[str, str] = {}

try:
    from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
    from google.cloud import logging
    from google.cloud.exceptions import GoogleCloudError

    # The overrides parameter needed to pass the dbt command was added in apache-airflow-providers-google==10.13.0
    init_signature = inspect.signature(CloudRunExecuteJobOperator.__init__)
    if "overrides" not in init_signature.parameters:
        raise AttributeError(
            "CloudRunExecuteJobOperator does not have `overrides` attribute. Ensure you've installed apache-airflow-providers-google of at least 10.11.0 "
            "separately or with `pip install astronomer-cosmos[...,gcp-cloud-run-job]`."
        )
except ImportError:
    raise ImportError(
        "Could not import CloudRunExecuteJobOperator. Ensure you've installed the Google Cloud provider "
        "separately or with `pip install astronomer-cosmos[...,gcp-cloud-run-job]`."
    )


class DbtGcpCloudRunJobBaseOperator(AbstractDbtBase, CloudRunExecuteJobOperator):  # type: ignore
    """
    Executes a dbt core cli command in a Cloud Run Job instance with dbt installed in it.

    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) + list(CloudRunExecuteJobOperator.template_fields)
    )

    intercept_flag = False

    def __init__(
        self,
        # arguments required by CloudRunExecuteJobOperator
        project_id: str,
        region: str,
        job_name: str,
        #
        profile_config: ProfileConfig | None = None,
        command: list[str] | None = None,
        environment_variables: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.profile_config = profile_config
        self.command = command
        self.environment_variables = environment_variables or DEFAULT_ENVIRONMENT_VARIABLES
        super().__init__(project_id=project_id, region=region, job_name=job_name, **kwargs)
        # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
        # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
        # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
        # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
        # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
        # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
        kwargs.update(
            {
                "project_id": project_id,
                "region": region,
                "job_name": job_name,
                "command": command,
                "environment_variables": environment_variables,
            }
        )
        base_operator_args = set(inspect.signature(CloudRunExecuteJobOperator.__init__).parameters.keys())
        base_kwargs = {}
        for arg_key, arg_value in kwargs.items():
            if arg_key in base_operator_args:
                base_kwargs[arg_key] = arg_value
        base_kwargs["task_id"] = kwargs["task_id"]
        CloudRunExecuteJobOperator.__init__(self, **base_kwargs)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
    ) -> Any:
        self.build_command(context, cmd_flags)
        self.log.info(f"Running command: {self.command}")
        result = CloudRunExecuteJobOperator.execute(self, context)

        # Pull Google Cloud Run job logs from Google Cloud Logging to Airflow logs
        execution_name = result["latest_created_execution"]["name"]
        execution_time = result["latest_created_execution"]["create_time"]
        filter_ = f'resource.type = "cloud_run_job" AND resource.labels.job_name = "{self.job_name}" AND timestamp>="{execution_time}"'

        self.log.info("Attempt to retrieve logs from Google Cloud Logging")
        time.sleep(5)  # Add sleep time to make sure all the job logs are available when we do the request

        # List to store log messages
        log_messages = []

        try:
            client = logging.Client(project=self.project_id)
            # Search for logs associated with the job name
            entries = client.list_entries(filter_=filter_)
            self.log.info(f"Listing logs of the execution {execution_name}:")
            if not entries:
                self.log.warning("No logs found for the Cloud Run job.")
            else:
                for entry in entries:
                    # Search for logs associated with the job executed
                    if entry.labels["run.googleapis.com/execution_name"] == execution_name:
                        log_messages.append(entry.payload)
                        self.log.info(f"Cloud Run Log: {entry.payload}")
                return log_messages

        except GoogleCloudError as e:
            # Catch Google Cloud-related errors (e.g., permission issues)
            self.log.warning(f"Warning: Error retrieving logs from Google Cloud Logging: {str(e)}")
            # Continue without raising an error, just log the warning

    def build_command(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)
        self.environment_variables = {**env_vars, **self.environment_variables}
        self.command = dbt_cmd
        # Override Cloud Run Job default arguments with dbt command
        self.overrides = {
            "container_overrides": [
                {
                    "args": self.command,
                    "env": [{"name": key, "value": value} for key, value in self.environment_variables.items()],
                }
            ],
        }


class DbtBuildGcpCloudRunJobOperator(DbtBuildMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtGcpCloudRunJobBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSGcpCloudRunJobOperator(DbtLSMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core ls command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedGcpCloudRunJobOperator(DbtSeedMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    template_fields: Sequence[str] = DbtGcpCloudRunJobBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotGcpCloudRunJobOperator(DbtSnapshotMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core snapshot command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunGcpCloudRunJobOperator(DbtRunMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtGcpCloudRunJobBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunOperationGcpCloudRunJobOperator(DbtRunOperationMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    template_fields: Sequence[str] = DbtGcpCloudRunJobBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtCloneGcpCloudRunJobOperator(DbtCloneMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core clone command.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtWarningGcpCloudRunJobOperator(DbtGcpCloudRunJobBaseOperator, ABC):
    def __init__(self, on_warning_callback: Callable[..., Any] | None = None, *args: Any, **kwargs: Any) -> None:
        if not on_warning_callback:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(*args, **kwargs)
            self.on_warning_callback = on_warning_callback

    def _handle_warnings(self, logs: list[str], context: Context) -> None:
        """
         Handles warnings by extracting log issues, creating additional context, and calling the
         on_warning_callback with the updated context.

        :param logs: The log list with the cleaned Cloud Run Job logs.
        :param context: The original airflow context in which the build and run command was executed.
        """
        test_names, test_results = extract_log_issues(logs)

        warning_context = dict(context)
        warning_context["test_names"] = test_names
        warning_context["test_results"] = test_results

        self.on_warning_callback(warning_context)

    def execute(self, context: Context, **kwargs: Any) -> None:
        result = self.build_and_run_cmd(context=context, cmd_flags=self.add_cmd_flags())
        log_list = [log for log in result if type(log) == str]  # clean log list with only string type values

        if not (
            isinstance(context["task_instance"], TaskInstance)
            and (
                isinstance(context["task_instance"].task, DbtTestGcpCloudRunJobOperator)
                or isinstance(context["task_instance"].task, DbtSourceGcpCloudRunJobOperator)
            )
        ):
            return

        should_trigger_callback = all(
            [
                log_list,
                self.on_warning_callback,
                DBT_NO_TESTS_MSG not in log_list[-2],
                DBT_WARN_MSG in log_list[-2],
            ]
        )

        if should_trigger_callback:
            warnings = int(log_list[-2].split(f"{DBT_WARN_MSG}=")[1].split()[0])
            if warnings > 0:
                self._handle_warnings(log_list, context)


class DbtTestGcpCloudRunJobOperator(DbtTestMixin, DbtWarningGcpCloudRunJobOperator):
    """
    Executes a dbt core test command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceGcpCloudRunJobOperator(DbtSourceMixin, DbtWarningGcpCloudRunJobOperator):
    """
    Executes a dbt core source freshness command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
