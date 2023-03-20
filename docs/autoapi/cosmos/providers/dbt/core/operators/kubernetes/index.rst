:py:mod:`cosmos.providers.dbt.core.operators.kubernetes`
========================================================

.. py:module:: cosmos.providers.dbt.core.operators.kubernetes


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.operators.kubernetes.DbtKubernetesBaseOperator
   cosmos.providers.dbt.core.operators.kubernetes.DbtLSKubernetesOperator
   cosmos.providers.dbt.core.operators.kubernetes.DbtSeedKubernetesOperator
   cosmos.providers.dbt.core.operators.kubernetes.DbtRunKubernetesOperator
   cosmos.providers.dbt.core.operators.kubernetes.DbtTestKubernetesOperator
   cosmos.providers.dbt.core.operators.kubernetes.DbtRunOperationKubernetesOperator
   cosmos.providers.dbt.core.operators.kubernetes.DbtDepsKubernetesOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.operators.kubernetes.logger


.. py:data:: logger



.. py:class:: DbtKubernetesBaseOperator(**kwargs)

   Bases: :py:obj:`airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator`, :py:obj:`cosmos.providers.dbt.core.operators.base.DbtBaseOperator`

   Executes a dbt core cli command in a Kubernetes Pod.


   .. py:attribute:: template_fields
      :type: Sequence[str]



   .. py:attribute:: intercept_flag
      :value: False



   .. py:method:: build_env_args(env: dict) -> list[kubernetes.client.models.V1EnvVar]


   .. py:method:: build_and_run_cmd(context: airflow.utils.context.Context, cmd_flags: list[str] | None = None)


   .. py:method:: build_kube_args(cmd_flags, context)



.. py:class:: DbtLSKubernetesOperator(**kwargs)

   Bases: :py:obj:`DbtKubernetesBaseOperator`

   Executes a dbt core ls command.

   .. py:attribute:: ui_color
      :value: '#DBCDF6'



   .. py:method:: execute(context: airflow.utils.context.Context)

      Based on the deferrable parameter runs the pod asynchronously or synchronously



.. py:class:: DbtSeedKubernetesOperator(full_refresh: bool = False, **kwargs)

   Bases: :py:obj:`DbtKubernetesBaseOperator`

   Executes a dbt core seed command.

   :param full_refresh: dbt optional arg - dbt will treat incremental models as table models

   .. py:attribute:: ui_color
      :value: '#F58D7E'



   .. py:method:: add_cmd_flags()


   .. py:method:: execute(context: airflow.utils.context.Context)

      Based on the deferrable parameter runs the pod asynchronously or synchronously



.. py:class:: DbtRunKubernetesOperator(**kwargs)

   Bases: :py:obj:`DbtKubernetesBaseOperator`

   Executes a dbt core run command.

   .. py:attribute:: ui_color
      :value: '#7352BA'



   .. py:attribute:: ui_fgcolor
      :value: '#F4F2FC'



   .. py:method:: execute(context: airflow.utils.context.Context)

      Based on the deferrable parameter runs the pod asynchronously or synchronously



.. py:class:: DbtTestKubernetesOperator(**kwargs)

   Bases: :py:obj:`DbtKubernetesBaseOperator`

   Executes a dbt core test command.

   .. py:attribute:: ui_color
      :value: '#8194E0'



   .. py:method:: execute(context: airflow.utils.context.Context)

      Based on the deferrable parameter runs the pod asynchronously or synchronously



.. py:class:: DbtRunOperationKubernetesOperator(macro_name: str, args: dict = None, **kwargs)

   Bases: :py:obj:`DbtKubernetesBaseOperator`

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

      Based on the deferrable parameter runs the pod asynchronously or synchronously



.. py:class:: DbtDepsKubernetesOperator(**kwargs)

   Bases: :py:obj:`DbtKubernetesBaseOperator`

   Executes a dbt core deps command.

   .. py:attribute:: ui_color
      :value: '#8194E0'



   .. py:method:: execute(context: airflow.utils.context.Context)

      Based on the deferrable parameter runs the pod asynchronously or synchronously
