from typing import List

from pydantic import BaseModel, Field


class Task(BaseModel):
    """
    A task represents a single node in the DAG.

    :param task_id: The human-readable, unique identifier of the task
    :type task_id: str
    :param operator_class: The name of the operator class to use for this task
    :type operator_class: str
    :param upstream_task_ids: The task_ids of the tasks that this task is upstream of
    :type upstream_task_ids: List[str]
    :param arguments: The arguments to pass to the operator
    :type arguments: dict
    """

    task_id: str = Field(..., description="The human-readable, unique identifier of the task")

    operator_class: str = Field(
        ...,
        description="The name of the operator class to use for this task",
    )

    upstream_task_ids: List[str] = Field(
        [],
        description="The task_ids of the tasks that this task is upstream of",
    )

    arguments: dict = Field(
        {},
        description="The arguments to pass to the operator",
    )
