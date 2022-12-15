import importlib
from datetime import datetime
from pydantic import BaseModel, Field
import logging

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from cosmos.core.graph.entities import Group, Task

logger = logging.getLogger(__name__)


class CosmosDag(BaseModel):
    """
    Render a Task or Group as an Airflow DAG.
    """

    group: Group = Field(
        ...,
        description="The Group to render",
    )

    def render(self):
        """
        Render the DAG.

        :return: The rendered DAG
        :rtype: DAG
        """
        dag = DAG(
            dag_id=self.group.id,
            default_args={
                "owner": "airflow",
                "start_date": datetime(2019, 1, 1),
            },
            schedule_interval=None,
        )

        entities = {}

        print(self.group)

        for ent in self.group.entities:
            if isinstance(ent, Group):
                entities[ent.id] = CosmosTaskGroup(
                    group=ent,
                    dag=dag
                ).render()
            else:
                entities[ent.id] = CosmosOperator(
                    task=ent,
                    dag=dag,
                ).render()

        # add dependencies
        for ent in self.group.entities:
            print(f"Adding dependencies for {ent.id}...")
            for upstream_id in ent.upstream_entity_ids:
                print(f"Setting {upstream_id} as upstream for {ent.id}...")
                entities[upstream_id] >> entities[ent.id]

        return dag


class CosmosTaskGroup(BaseModel):
    """
    Render a Group as an Airflow TaskGroup.
    """

    # this is required to allow Airflow types to be passed to the model
    class Config:
        arbitrary_types_allowed = True

    group: Group = Field(
        ...,
        description="The Group to render",
    )

    dag: DAG = Field(
        ...,
        description="The DAG to render the Group into",
    )

    task_group: TaskGroup = Field(
        None,
        description="The TaskGroup to render the Group into",
    )

    def render(self):
        """
        Render the TaskGroup.

        :return: The rendered TaskGroup
        :rtype: TaskGroup
        """
        # first, instantiate the TaskGroup
        task_group = TaskGroup(
            group_id=self.group.id,
            dag=self.dag,
            parent_group=self.task_group,
        )

        entities = {}

        # then, render all the entities in the group
        for ent in self.group.entities:
            if isinstance(ent, Group):
                CosmosTaskGroup(
                    group=ent,
                    dag=self.dag,
                    task_group=task_group,
                ).render()
            else:
                entities[ent.id] = CosmosOperator(
                    task=ent,
                    dag=self.dag,
                    task_group=task_group,
                ).render()

        # add dependencies
        for ent in self.group.entities:
            for upstream_id in ent.upstream_entity_ids:
                entities[upstream_id] >> entities[ent.id]
        
        return task_group


class CosmosOperator(BaseModel):
    """
    Render a Task as an Airflow Operator.
    """

    # this is required to allow Airflow types to be passed to the model
    class Config:
        arbitrary_types_allowed = True


    task: Task = Field(
        ...,
        description="The Task to render",
    )

    dag: DAG = Field(
        ...,
        description="The DAG to render the Task into",
    )

    task_group: TaskGroup = Field(
        None,
        description="The TaskGroup to render the Task into",
    )

    def render(self):
        """
        Render the Task.

        :param task: The Task to render
        :type task: Task

        :return: The rendered Task's Operator
        :rtype: BaseOperator
        """
        # first, import the operator class from the
        # fully qualified name defined in the task
        module_name, class_name = self.task.operator_class.rsplit(".", 1)
        module = importlib.import_module(module_name)
        Operator = getattr(module, class_name)

        # then, instantiate the operator with the arguments
        # defined in the task, along with the task_id and dag
        return Operator(
            task_id=self.task.id,
            dag=self.dag,
            task_group=self.task_group,
            **self.task.arguments
        )
        