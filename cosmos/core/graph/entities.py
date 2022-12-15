from typing import List

from pydantic import BaseModel, Field

import logging

logger = logging.getLogger(__name__)


class CosmosEntity(BaseModel):
    """
    A CosmosEntity defines a base class for all Cosmos entities.

    :param id: The human-readable, unique identifier of the entity
    :type id: str
    :param upstream_entity_ids: The ids of the entities that this entity is upstream of
    :type upstream_entity_ids: List[str]
    """

    id: str = Field(..., description="The human-readable, unique identifier of the entity")

    upstream_entity_ids: List[str] = Field(
        [],
        description="The ids of the entities that this entity is upstream of",
    )

    def add_upstream(self, entity: "CosmosEntity"):
        """
        Add an upstream entity to the entity.

        :param entity: The entity to add
        :type entity: CosmosEntity
        """
        self.upstream_entity_ids.append(entity.id)


class Group(CosmosEntity):
    """
    A Group represents a collection of entities that are connected by dependencies.
    """

    entities: List[CosmosEntity] = Field(
        [],
        description="The list of entities in the group",
    )

    def add_entity(self, entity: CosmosEntity):
        """
        Add an entity to the group.

        :param entity: The entity to add
        :type entity: CosmosEntity
        """
        logger.info(f"Adding entity {entity.id} to group {self.id}...")

        self.entities.append(entity)


class Task(CosmosEntity):
    """
    A task represents a single node in the DAG.

    :param operator_class: The name of the operator class to use for this task
    :type operator_class: str
    :param arguments: The arguments to pass to the operator
    :type arguments: dict
    """

    operator_class: str = Field(
        ...,
        description="The name of the operator class to use for this task",
    )

    arguments: dict = Field(
        {},
        description="The arguments to pass to the operator",
    )