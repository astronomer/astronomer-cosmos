:py:mod:`cosmos.providers.dbt.core.operators.local`
===================================================

.. py:module:: cosmos.providers.dbt.core.operators.local


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.operators.local.DbtLocalBaseOperator
   cosmos.providers.dbt.core.operators.local.DbtLSLocalOperator
   cosmos.providers.dbt.core.operators.local.DbtSeedLocalOperator
   cosmos.providers.dbt.core.operators.local.DbtRunLocalOperator
   cosmos.providers.dbt.core.operators.local.DbtTestLocalOperator
   cosmos.providers.dbt.core.operators.local.DbtRunOperationLocalOperator
   cosmos.providers.dbt.core.operators.local.DbtDepsLocalOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.operators.local.logger


.. py:data:: logger



.. py:class:: DbtLocalBaseOperator(**kwargs)

   Bases: :py:obj:`cosmos.providers.dbt.core.operators.base.DbtBaseOperator`

   Executes a dbt core cli command locally.


   .. py:attribute:: template_fields
      :type: Sequence[str]



   .. py:method:: subprocess_hook()

      Returns hook for running the bash command.


   .. py:method:: exception_handling(result: airflow.hooks.subprocess.SubprocessResult)


   .. py:method:: run_command(cmd: list[str], env: dict[str, str]) -> airflow.hooks.subprocess.SubprocessResult


   .. py:method:: build_and_run_cmd(context: airflow.utils.context.Context, cmd_flags: list[str] | None = None) -> airflow.hooks.subprocess.SubprocessResult


   .. py:method:: execute(context: airflow.utils.context.Context) -> str

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: on_kill() -> None

      Override this method to clean up subprocesses when a task instance
      gets killed. Any use of the threading, subprocess or multiprocessing
      module within an operator needs to be cleaned up, or it will leave
      ghost processes behind.



.. py:class:: DbtLSLocalOperator(**kwargs)

   Bases: :py:obj:`DbtLocalBaseOperator`

   Executes a dbt core ls command.

   .. py:attribute:: ui_color
      :value: '#DBCDF6'



   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: DbtSeedLocalOperator(full_refresh: bool = False, **kwargs)

   Bases: :py:obj:`DbtLocalBaseOperator`

   Executes a dbt core seed command.

   :param full_refresh: dbt optional arg - dbt will treat incremental models as table models

   .. py:attribute:: ui_color
      :value: '#F58D7E'



   .. py:method:: add_cmd_flags()


   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: DbtRunLocalOperator(**kwargs)

   Bases: :py:obj:`DbtLocalBaseOperator`

   Executes a dbt core run command.

   .. py:attribute:: ui_color
      :value: '#7352BA'



   .. py:attribute:: ui_fgcolor
      :value: '#F4F2FC'



   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: DbtTestLocalOperator(**kwargs)

   Bases: :py:obj:`DbtLocalBaseOperator`

   Executes a dbt core test command.

   .. py:attribute:: ui_color
      :value: '#8194E0'



   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: DbtRunOperationLocalOperator(macro_name: str, args: dict = None, **kwargs)

   Bases: :py:obj:`DbtLocalBaseOperator`

   Executes a dbt core run-operation command.

   :param macro_name: name of macro to execute
   :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
       selected macro.

   .. py:attribute:: ui_color
      :value: '#8194E0'



   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: 'args'



   .. py:method:: add_cmd_flags()


   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: DbtDepsLocalOperator(**kwargs)

   Bases: :py:obj:`DbtLocalBaseOperator`

   Executes a dbt core deps command.

   .. py:attribute:: ui_color
      :value: '#8194E0'



   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
