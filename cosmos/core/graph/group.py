from typing import List

from cosmos.core.graph.task import Task
from pydantic import BaseModel, Field


class Group(BaseModel):
    """
    A Group represents a collection of tasks that are connected by dependencies.

    :param tasks: The list of tasks in the DAG
    :type tasks: List[Task]
    """

    group_id: str = Field(
        ...,
        description="The human-readable, unique identifier of the group",
    )

    tasks: List[Task] = Field(
        ...,
        description="The list of tasks in the DAG",
    )


    def __init__(self, id: str, tasks: List[Task]):
        self.id = id
        self.tasks = tasks
    
    def add_task(self, task: Task):
        self.tasks.append(task)
