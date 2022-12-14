import importlib
from datetime import datetime

from airflow.models import DAG
from cosmos.core.graph.group import Group


class CosmosDag:
    """
    Render a Task or Group as an Airflow DAG.
    """

    def render(self, group: Group):
        """
        Render the DAG.

        :return: The rendered DAG
        :rtype: DAG
        """
        dag = DAG(
            dag_id=group.group_id,
            default_args={
                "owner": "airflow",
                "start_date": datetime(2019, 1, 1),
            },
            schedule_interval=None,
        )

        tasks = {}

        for task in group.tasks:
            # import the operator class
            module_name, class_name = task.operator_class.rsplit(".", 1)
            module = importlib.import_module(module_name)
            operator = getattr(module, class_name)

            # instantiate the operator
            t = operator(task_id=task.task_id, dag=dag)

            # add the task to the DAG
            tasks[task.task_id] = t

        # add dependencies
        for task in group.tasks:
            for upstream_task_id in task.upstream_task_ids:
                tasks[task.task_id] << tasks[upstream_task_id]

        return dag
