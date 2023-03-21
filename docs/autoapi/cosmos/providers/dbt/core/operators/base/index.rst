:py:mod:`cosmos.providers.dbt.core.operators.base`
==================================================

.. py:module:: cosmos.providers.dbt.core.operators.base


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.operators.base.DbtBaseOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.operators.base.logger


.. py:data:: logger



.. py:class:: DbtBaseOperator(project_dir: str, conn_id: str, base_cmd: str | list[str] = None, select: str = None, exclude: str = None, selector: str = None, vars: dict = None, models: str = None, cache_selected_only: bool = False, no_version_check: bool = False, fail_fast: bool = False, quiet: bool = False, warn_error: bool = False, db_name: str = None, schema: str = None, env: dict = None, append_env: bool = False, output_encoding: str = 'utf-8', skip_exit_code: int = 99, cancel_query_on_kill: bool = True, dbt_executable_path: str = 'dbt', dbt_cmd_flags: Dict[str, Any] = {}, **kwargs)

   Bases: :py:obj:`airflow.models.baseoperator.BaseOperator`

   Executes a dbt core cli command.

   :param project_dir: Which directory to look in for the dbt_project.yml file. Default is the current working
   directory and its parents.
   :param conn_id: The airflow connection to use as the target
   :param base_cmd: dbt sub-command to run (i.e ls, seed, run, test, etc.)
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
   :param fail_fast: dbt optional argument to make dbt exit immediately if a single resource fails to build.
   :param quiet: dbt optional argument to show only error logs in stdout
   :param warn_error: dbt optional argument to convert dbt warnings into errors
   :param db_name: override the target db instead of the one supplied in the airflow connection
   :param schema: override the target schema instead of the one supplied in the airflow connection
   :param env: If env is not None, it must be a dict that defines the
       environment variables for the new process; these are used instead
       of inheriting the current process environment, which is the default
       behavior. (templated)
   :param append_env: If False(default) uses the environment variables passed in env params
       and does not inherit the current process environment. If True, inherits the environment variables
       from current passes and then environment variable passed by the user will either update the existing
       inherited environment variables or the new variables gets appended to it
   :param output_encoding: Output encoding of bash command
   :param skip_exit_code: If task exits with this exit code, leave the task
       in ``skipped`` state (default: 99). If set to ``None``, any non-zero
       exit code will be treated as a failure.
   :param cancel_query_on_kill: If true, then cancel any running queries when the task's on_kill() is executed.
       Otherwise, the query will keep running when the task is killed.
   :param dbt_executable_path: Path to dbt executable can be used with venv
       (i.e. /home/astro/.pyenv/versions/dbt_venv/bin/dbt)
   :param dbt_cmd_flags: Flags passed to dbt command override those that are calculated.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('env', 'vars')



   .. py:attribute:: global_flags
      :value: ('project_dir', 'select', 'exclude', 'selector', 'vars', 'models', 'profiles_dir', 'profile')



   .. py:attribute:: global_boolean_flags
      :value: ('no_version_check', 'cache_selected_only', 'fail_fast', 'quiet', 'warn_error')



   .. py:attribute:: intercept_flag
      :value: True



   .. py:method:: get_env(context: airflow.utils.context.Context, profile_vars: dict[str, str]) -> dict[str, str]

      Builds the set of environment variables to be exposed for the bash command.
      The order of determination is:
          1. Environment variables created for dbt profiles, `profile_vars`.
          2. The Airflow context as environment variables.
          3. System environment variables if dbt_args{"append_env": True}
          4. User specified environment variables, through dbt_args{"vars": {"key": "val"}}
      If a user accidentally uses a key that is found earlier in the determination order then it is overwritten.


   .. py:method:: add_global_flags() -> list[str]


   .. py:method:: build_cmd(context: airflow.utils.context.Context, cmd_flags: list[str] | None = None, handle_profile: bool = True) -> Tuple[list[str], dict]
