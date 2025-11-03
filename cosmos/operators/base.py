from __future__ import annotations

import inspect
import logging
import os
from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence, Tuple

import yaml
from airflow.utils.context import context_merge

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    from airflow.utils.operator_helpers import context_to_airflow_vars  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    from airflow.sdk.execution_time.context import context_to_airflow_vars  # type: ignore

from airflow.utils.strings import to_boolean

from cosmos.dbt.executable import get_system_dbt
from cosmos.log import get_logger


def _sanitize_xcom_key(file_path: str) -> str:
    return file_path.replace("/", "_").replace("\\", "_")


class AbstractDbtBase(metaclass=ABCMeta):
    """
    Executes a dbt core cli command.

    :param project_dir: Which directory to look in for the dbt_project.yml file. Default is the current working
    directory and its parents.
    :param conn_id: The airflow connection to use as the target
    :param select: dbt optional argument that specifies which nodes to include.
    :param exclude: dbt optional argument that specifies which models to exclude.
    :param selector: dbt optional argument - the selector name to use, as defined in selectors.yml
    :param vars: dbt optional argument - Supply variables to the project. This argument overrides variables
        defined in your dbt_project.yml file. This argument should be a YAML
        string, eg. '{my_variable: my_value}' (templated)
    :param models: dbt optional argument that specifies which nodes to include.
    :param cache_selected_only:
    :param no_version_check: dbt optional argument - If set, skip ensuring dbt's version matches the one specified in
        the dbt_project.yml file ('require-dbt-version')
    :param emit_datasets: Enable emitting inlets and outlets during task execution
    :param fail_fast: dbt optional argument to make dbt exit immediately if a single resource fails to build.
    :param quiet: dbt optional argument to show only error logs in stdout
    :param warn_error: dbt optional argument to convert dbt warnings into errors
    :param db_name: override the target db instead of the one supplied in the airflow connection
    :param schema: override the target schema instead of the one supplied in the airflow connection
    :param env: If env is not None, it must be a dict that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :param append_env: If True, inherits the environment variables
        from current process and then environment variable passed by the user will either update the existing
        inherited environment variables or the new variables gets appended to it.
        If False (default), only uses the environment variables passed in env params
        and does not inherit the current process environment.
    :param output_encoding: Output encoding of bash command
    :param skip_exit_code: If task exits with this exit code, leave the task
        in ``skipped`` state (default: 99). If set to ``None``, any non-zero
        exit code will be treated as a failure.
    :param partial_parse: If True (default), then the operator will use the
        ``partial_parse.msgpack`` during execution if it exists. If False, then
        a flag will be explicitly set to turn off partial parsing.
    :param cancel_query_on_kill: If true, then cancel any running queries when the task's on_kill() is executed.
        Otherwise, the query will keep running when the task is killed.
    :param dbt_executable_path: Path to dbt executable can be used with venv
        (i.e. /home/astro/.pyenv/versions/dbt_venv/bin/dbt)
    :param dbt_cmd_flags: List of flags to pass to dbt command
    :param dbt_cmd_global_flags: List of dbt global flags to be passed to the dbt command
    :param cache_dir: Directory used to cache Cosmos/dbt artifacts in Airflow worker nodes
    :param extra_context: A dictionary of values to add to the TaskInstance's Context
    """

    template_fields: Sequence[str] = ("env", "select", "exclude", "selector", "vars", "models", "dbt_cmd_flags")
    global_flags = (
        "project_dir",
        "select",
        "exclude",
        "selector",
        "vars",
        "models",
    )
    global_boolean_flags = ("no_version_check", "cache_selected_only", "fail_fast", "quiet", "warn_error")

    intercept_flag = True

    @property
    @abstractmethod
    def base_cmd(self) -> list[str]:
        """Override this property to set the dbt sub-command (i.e ls, seed, run, test, etc.) for the operator"""

    def __init__(
        self,
        project_dir: str,
        conn_id: str | None = None,
        select: str | None = None,
        exclude: str | None = None,
        selector: str | None = None,
        vars: dict[str, str] | None = None,
        models: str | None = None,
        emit_datasets: bool = True,
        indirect_selection: str | None = None,
        cache_selected_only: bool = False,
        no_version_check: bool = False,
        fail_fast: bool = False,
        quiet: bool = False,
        warn_error: bool = False,
        db_name: str | None = None,
        schema: str | None = None,
        env: dict[str, Any] | None = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_exit_code: int = 99,
        partial_parse: bool = True,
        cancel_query_on_kill: bool = True,
        dbt_executable_path: str = get_system_dbt(),
        dbt_cmd_flags: list[str] | None = None,
        dbt_cmd_global_flags: list[str] | None = None,
        cache_dir: Path | None = None,
        extra_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.project_dir = project_dir
        self.conn_id = conn_id
        self.select = select
        self.exclude = exclude
        self.selector = selector
        self.vars = vars
        self.models = models
        self.emit_datasets = emit_datasets
        self.indirect_selection = indirect_selection
        self.cache_selected_only = cache_selected_only
        self.no_version_check = no_version_check
        self.fail_fast = fail_fast
        self.quiet = quiet
        self.warn_error = warn_error
        self.db_name = db_name
        self.schema = schema
        self.env = env
        self.append_env = append_env
        self.output_encoding = output_encoding
        self.skip_exit_code = skip_exit_code
        self.partial_parse = partial_parse
        self.cancel_query_on_kill = cancel_query_on_kill
        self.dbt_executable_path = dbt_executable_path
        self.dbt_cmd_flags = dbt_cmd_flags or []
        self.dbt_cmd_global_flags = dbt_cmd_global_flags or []
        self.cache_dir = cache_dir
        self.extra_context = extra_context or {}
        kwargs.pop("full_refresh", None)  # usage of this param should be implemented in child classes

    # The following is necessary so that dynamic mapped classes work since Cosmos 1.9.0 subclass changes
    # Bug report: https://github.com/astronomer/astronomer-cosmos/issues/1546
    __init__._BaseOperatorMeta__param_names = {  # type: ignore
        name
        for (name, param) in inspect.signature(__init__).parameters.items()
        if param.name != "self" and param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD)
    }

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        # The following is necessary so that dynamic mapped classes work since Cosmos 1.9.0 subclass changes
        # Since this class is subclassed by all Cosmos operators, to do this here allows to avoid to have this
        # logic explicitly in all subclasses
        # Bug report: https://github.com/astronomer/astronomer-cosmos/issues/1546
        cls.__init__._BaseOperatorMeta__param_names = {  # type: ignore
            name
            for (name, param) in inspect.signature(cls.__init__).parameters.items()
            if param.name != "self" and param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD)
        }

    def get_env(self, context: Context) -> dict[str, str | bytes | os.PathLike[Any]]:
        """
        Builds the set of environment variables to be exposed for the bash command.

        The order of determination is:
            1. If append_env is True, the current process environment.
            2. The Airflow context as environment variables.
            3. The env parameter passed to the Operator

        Note that this also filters out any invalid types that cannot be cast to strings.
        """
        env: dict[str, Any] = {}

        if self.append_env:
            env.update(os.environ)

        # Airflow context as environment variables
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        env.update(airflow_context_vars)

        # env parameter passed to the Operator
        if self.env and isinstance(self.env, dict):
            env.update(self.env)

        # filter out invalid types and give a warning when a value is removed
        accepted_types = (str, bytes, os.PathLike)

        filtered_env: dict[str, str | bytes | os.PathLike[Any]] = {}

        for key, val in env.items():
            if isinstance(key, accepted_types) and isinstance(val, accepted_types):
                filtered_env[key] = val
            else:
                if isinstance(key, accepted_types):
                    self.log.warning(
                        "Env var %s was ignored because its key is not a valid type. Must be one of %s",
                        key,
                        accepted_types,
                    )

                if isinstance(val, accepted_types):
                    self.log.warning(
                        "Env var %s was ignored because its value is not a valid type. Must be one of %s",
                        key,
                        accepted_types,
                    )

        return filtered_env

    @property
    def log(self) -> logging.Logger:
        return get_logger(__name__)

    def add_global_flags(self) -> list[str]:
        flags = []
        for global_flag in self.global_flags:
            # for now, skip the project_dir flag
            if global_flag == "project_dir":
                continue

            dbt_name = f"--{global_flag.replace('_', '-')}"
            global_flag_value = self.__getattribute__(global_flag)
            flags.extend(self._process_global_flag(dbt_name, global_flag_value))

        for global_boolean_flag in self.global_boolean_flags:
            if self.__getattribute__(global_boolean_flag):
                flags.append(f"--{global_boolean_flag.replace('_', '-')}")
        return flags

    @staticmethod
    def _process_global_flag(flag_name: str, flag_value: Any) -> list[str]:
        """Helper method to process global flags and reduce complexity."""
        if flag_value is None:
            return []
        elif isinstance(flag_value, dict):
            yaml_string = yaml.dump(flag_value)
            return [flag_name, yaml_string]
        elif isinstance(flag_value, list) and flag_value:
            return [flag_name, " ".join(flag_value)]
        elif isinstance(flag_value, list):
            return []
        else:
            return [flag_name, str(flag_value)]

    def add_cmd_flags(self) -> list[str]:
        """Allows subclasses to override to add flags for their dbt command"""
        return []

    def build_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
    ) -> Tuple[list[str], dict[str, str | bytes | os.PathLike[Any]]]:
        dbt_cmd = [self.dbt_executable_path]

        dbt_cmd.extend(self.dbt_cmd_global_flags)

        if not self.partial_parse:
            dbt_cmd.append("--no-partial-parse")

        dbt_cmd.extend(self.base_cmd)

        if self.indirect_selection:
            dbt_cmd += ["--indirect-selection", self.indirect_selection]

        dbt_cmd.extend(self.add_global_flags())

        # add command specific flags
        if cmd_flags:
            dbt_cmd.extend(cmd_flags)

        # add user-supplied args
        if self.dbt_cmd_flags:
            # Filter out empty strings that might result from template rendering
            filtered_flags = [flag for flag in self.dbt_cmd_flags if flag and str(flag).strip()]
            dbt_cmd.extend(filtered_flags)

        env = self.get_env(context)

        return dbt_cmd, env

    @abstractmethod
    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str],
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Override this method for the operator to execute the dbt command"""

    def execute(self, context: Context, **kwargs) -> Any | None:  # type: ignore
        if self.extra_context:
            context_merge(context, self.extra_context)

        self.build_and_run_cmd(context=context, cmd_flags=self.add_cmd_flags(), **kwargs)


class DbtBuildMixin:
    """Mixin for dbt build command."""

    base_cmd = ["build"]
    ui_color = "#8194E0"

    template_fields: Sequence[str] = ("full_refresh",)

    def __init__(self, full_refresh: bool | str = False, **kwargs: Any) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)

    def add_cmd_flags(self) -> list[str]:
        flags = []

        if isinstance(self.full_refresh, str):
            # Handle template fields when render_template_as_native_obj=False
            full_refresh = to_boolean(self.full_refresh)
        else:
            full_refresh = self.full_refresh

        if full_refresh is True:
            flags.append("--full-refresh")

        return flags


class DbtLSMixin:
    """
    Executes a dbt core ls command.
    """

    base_cmd = ["ls"]
    ui_color = "#DBCDF6"


class DbtSeedMixin:
    """
    Mixin for dbt seed operation command.

    :param full_refresh: whether to add the flag --full-refresh to the dbt seed command
    """

    base_cmd = ["seed"]
    ui_color = "#F58D7E"

    template_fields: Sequence[str] = ("full_refresh",)

    def __init__(self, full_refresh: bool | str = False, **kwargs: Any) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)

    def add_cmd_flags(self) -> list[str]:
        flags = []

        if isinstance(self.full_refresh, str):
            # Handle template fields when render_template_as_native_obj=False
            full_refresh = to_boolean(self.full_refresh)
        else:
            full_refresh = self.full_refresh

        if full_refresh is True:
            flags.append("--full-refresh")

        return flags


class DbtSnapshotMixin:
    """Mixin for a dbt snapshot command."""

    base_cmd = ["snapshot"]
    ui_color = "#964B00"


class DbtSourceMixin:
    """
    Executes a dbt source freshness command.
    """

    base_cmd = ["source", "freshness"]
    ui_color = "#34CCEB"


class DbtRunMixin:
    """
    Mixin for dbt run command.

    :param full_refresh: whether to add the flag --full-refresh to the dbt seed command
    """

    base_cmd = ["run"]
    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"

    template_fields: Sequence[str] = ("full_refresh",)

    def __init__(self, full_refresh: bool | str = False, **kwargs: Any) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)

    def add_cmd_flags(self) -> list[str]:
        flags = []

        if isinstance(self.full_refresh, str):
            # Handle template fields when render_template_as_native_obj=False
            full_refresh = to_boolean(self.full_refresh)
        else:
            full_refresh = self.full_refresh

        if full_refresh is True:
            flags.append("--full-refresh")

        return flags


class DbtTestMixin:
    """Mixin for dbt test command."""

    base_cmd = ["test"]
    ui_color = "#8194E0"

    def __init__(
        self,
        exclude: str | None = None,
        select: str | None = None,
        selector: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.select = select
        self.exclude = exclude
        self.selector = selector
        super().__init__(exclude=exclude, select=select, selector=selector, **kwargs)  # type: ignore


class DbtRunOperationMixin:
    """
    Mixin for dbt run operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    ui_color = "#8194E0"
    template_fields: Sequence[str] = ("args",)

    def __init__(self, macro_name: str, args: dict[str, Any] | None = None, **kwargs: Any) -> None:
        self.macro_name = macro_name
        self.args = args
        super().__init__(**kwargs)

    @property
    def base_cmd(self) -> list[str]:
        return ["run-operation", self.macro_name]

    def add_cmd_flags(self) -> list[str]:
        flags = []
        if self.args is not None:
            flags.append("--args")
            flags.append(yaml.dump(self.args))
        return flags


class DbtCompileMixin:
    """
    Mixin for dbt compile command.
    """

    base_cmd = ["compile"]
    ui_color = "#877c7c"


class DbtCloneMixin:
    """Mixin for dbt clone command."""

    base_cmd = ["clone"]
    ui_color = "#83a300"

    def __init__(self, full_refresh: bool | str = False, **kwargs: Any) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)

    def add_cmd_flags(self) -> list[str]:
        flags = []

        if isinstance(self.full_refresh, str):
            # Handle template fields when render_template_as_native_obj=False
            full_refresh = to_boolean(self.full_refresh)
        else:
            full_refresh = self.full_refresh

        if full_refresh is True:
            flags.append("--full-refresh")

        return flags
