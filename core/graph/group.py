from typing import List

from core.types.task import Task
from pydantic import BaseModel, Field


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
