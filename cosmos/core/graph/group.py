from typing import List

from pydantic import BaseModel, Field

from cosmos.core.graph.task import Task


class Group(BaseModel):
    """
    A Group represents a collection of tasks that are connected by dependencies.

    :param group_id: The human-readable, unique identifier of the group
    :type id: str
    :param tasks: The list of tasks in the DAG
    :type tasks: Optional[List[Task]]

    """

    group_id: str = Field(
        ...,
        description="The human-readable, unique identifier of the task",
    )

    tasks: List[Task] = Field(
        [],
        description="The list of tasks in the DAG",
    )

    def add_task(self, task: Task):
        self.tasks.append(task)
