:py:mod:`cosmos.providers.dbt.core.operators.docker`
====================================================

.. py:module:: cosmos.providers.dbt.core.operators.docker


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.operators.docker.DbtDockerBaseOperator
   cosmos.providers.dbt.core.operators.docker.DbtLSDockerOperator
   cosmos.providers.dbt.core.operators.docker.DbtSeedDockerOperator
   cosmos.providers.dbt.core.operators.docker.DbtRunDockerOperator
   cosmos.providers.dbt.core.operators.docker.DbtTestDockerOperator
   cosmos.providers.dbt.core.operators.docker.DbtRunOperationDockerOperator
   cosmos.providers.dbt.core.operators.docker.DbtDepsDockerOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.operators.docker.logger


.. py:data:: logger



.. py:class:: DbtDockerBaseOperator(image: str, **kwargs)

   Bases: :py:obj:`airflow.providers.docker.operators.docker.DockerOperator`, :py:obj:`cosmos.providers.dbt.core.operators.base.DbtBaseOperator`

   Executes a dbt core cli command in a Docker container.


   .. py:attribute:: template_fields
      :type: Sequence[str]



   .. py:attribute:: intercept_flag
      :value: False



   .. py:method:: build_and_run_cmd(context: airflow.utils.context.Context, cmd_flags: list[str] | None = None)


   .. py:method:: build_command(cmd_flags, context)



.. py:class:: DbtLSDockerOperator(**kwargs)

   Bases: :py:obj:`DbtDockerBaseOperator`

   Executes a dbt core ls command.

   .. py:attribute:: ui_color
      :value: '#DBCDF6'



   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: DbtSeedDockerOperator(full_refresh: bool = False, **kwargs)

   Bases: :py:obj:`DbtDockerBaseOperator`

   Executes a dbt core seed command.

   :param full_refresh: dbt optional arg - dbt will treat incremental models as table models

   .. py:attribute:: ui_color
      :value: '#F58D7E'



   .. py:method:: add_cmd_flags()


   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: DbtRunDockerOperator(**kwargs)

   Bases: :py:obj:`DbtDockerBaseOperator`

   Executes a dbt core run command.

   .. py:attribute:: ui_color
      :value: '#7352BA'



   .. py:attribute:: ui_fgcolor
      :value: '#F4F2FC'



   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: DbtTestDockerOperator(**kwargs)

   Bases: :py:obj:`DbtDockerBaseOperator`

   Executes a dbt core test command.

   .. py:attribute:: ui_color
      :value: '#8194E0'



   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: DbtRunOperationDockerOperator(macro_name: str, args: dict = None, **kwargs)

   Bases: :py:obj:`DbtDockerBaseOperator`

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



.. py:class:: DbtDepsDockerOperator(**kwargs)

   Bases: :py:obj:`DbtDockerBaseOperator`

   Executes a dbt core deps command.

   .. py:attribute:: ui_color
      :value: '#8194E0'



   .. py:method:: execute(context: airflow.utils.context.Context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
