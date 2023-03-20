:py:mod:`cosmos.core.airflow`
=============================

.. py:module:: cosmos.core.airflow


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cosmos.core.airflow.CosmosDag
   cosmos.core.airflow.CosmosTaskGroup



Functions
~~~~~~~~~

.. autoapisummary::

   cosmos.core.airflow.get_airflow_task



Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.core.airflow.logger


.. py:data:: logger



.. py:class:: CosmosDag(cosmos_group: cosmos.core.graph.entities.Group, *args: Any, **kwargs: Any)

   Bases: :py:obj:`airflow.models.dag.DAG`

   Render a Group as an Airflow DAG. Subclass of Airflow DAG.


.. py:class:: CosmosTaskGroup(cosmos_group: cosmos.core.graph.entities.Group, dag: Optional[airflow.models.dag.DAG] = None, *args: Any, **kwargs: Any)

   Bases: :py:obj:`airflow.utils.task_group.TaskGroup`

   Render a Group as an Airflow TaskGroup. Subclass of Airflow TaskGroup.


.. py:function:: get_airflow_task(task: cosmos.core.graph.entities.Task, dag: airflow.models.dag.DAG, task_group: Optional[airflow.utils.task_group.TaskGroup] = None) -> airflow.models.BaseOperator

   Get the Airflow Operator class for a Task.

   :param task: The Task to get the Operator for

   :return: The Operator class
   :rtype: BaseOperator
