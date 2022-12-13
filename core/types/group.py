from typing import List

from pydantic import BaseModel, Field

from core.types.task import Task


class Group(BaseModel):
    """
    A Group represents a collection of tasks that are connected by dependencies.

    :param tasks: The list of tasks in the DAG
    :type tasks: List[Task]
    """

    tasks: List[Task] = Field(
        ...,
        description="The list of tasks in the DAG",
    )
