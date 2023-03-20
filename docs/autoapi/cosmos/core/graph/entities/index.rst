:py:mod:`cosmos.core.graph.entities`
====================================

.. py:module:: cosmos.core.graph.entities


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cosmos.core.graph.entities.CosmosEntity
   cosmos.core.graph.entities.Group
   cosmos.core.graph.entities.Task




Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.core.graph.entities.logger


.. py:data:: logger



.. py:class:: CosmosEntity

   A CosmosEntity defines a base class for all Cosmos entities.

   :param id: The human-readable, unique identifier of the entity
   :param upstream_entity_ids: The ids of the entities that this entity is upstream of

   .. py:attribute:: id
      :type: str



   .. py:attribute:: upstream_entity_ids
      :type: List[str]



   .. py:method:: add_upstream(entity: CosmosEntity) -> None

      Add an upstream entity to the entity.

      :param entity: The entity to add



.. py:class:: Group

   Bases: :py:obj:`CosmosEntity`

   A Group represents a collection of entities that are connected by dependencies.

   .. py:attribute:: entities
      :type: List[CosmosEntity]



   .. py:method:: add_entity(entity: CosmosEntity) -> None

      Add an entity to the group.

      :param entity: The entity to add



.. py:class:: Task

   Bases: :py:obj:`CosmosEntity`

   A task represents a single node in the DAG.

   :param operator_class: The name of the operator class to use for this task
   :param arguments: The arguments to pass to the operator

   .. py:attribute:: operator_class
      :type: str
      :value: 'airflow.operators.dummy.DummyOperator'



   .. py:attribute:: arguments
      :type: Dict[str, Any]
