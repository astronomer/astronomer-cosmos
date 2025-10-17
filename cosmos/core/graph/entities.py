from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

import airflow
from packaging.version import Version

from cosmos.log import get_logger

logger = get_logger(__name__)


AIRFLOW_VERSION = Version(airflow.__version__)


@dataclass
class CosmosEntity:
    """
    A CosmosEntity defines a base class for all Cosmos entities.

    :param id: The human-readable, unique identifier of the entity
    :param upstream_entity_ids: The ids of the entities that this entity is upstream of
    """

    id: str
    upstream_entity_ids: List[str] = field(default_factory=list)

    def add_upstream(self, entity: CosmosEntity) -> None:
        """
        Add an upstream entity to the entity.

        :param entity: The entity to add
        """
        self.upstream_entity_ids.append(entity.id)


@dataclass
class Group(CosmosEntity):
    """
    A Group represents a collection of entities that are connected by dependencies.
    """

    entities: List[CosmosEntity] = field(default_factory=list)

    def add_entity(self, entity: CosmosEntity) -> None:
        """
        Add an entity to the group.

        :param entity: The entity to add
        """
        logger.info(f"Adding entity {entity.id} to group {self.id}...")

        self.entities.append(entity)


@dataclass
class Task(CosmosEntity):
    """
    A task represents a single node in the DAG.

    :param operator_class: The name of the operator class to use for this task
    :param arguments: The arguments to pass to the operator
    """

    owner: str = ""
    operator_class: str = (
        "airflow.operators.empty.EmptyOperator"
        if AIRFLOW_VERSION < Version("3.0")
        else "airflow.providers.standard.operators.empty.EmptyOperator"
    )
    arguments: Dict[str, Any] = field(default_factory=dict)
    extra_context: Dict[str, Any] = field(default_factory=dict)
