import importlib
from datetime import datetime

from airflow.models import DAG
from core.graph.group import Group


class CosmosDag:
    """
    Render a Task or Group as an Airflow DAG.
    """

    def __init__(self, group: Group):
        """
        :param group: The Group to render
        :type group: Group
        """
        self.group = group

    def render(self):
        """
        Render the DAG.

        :return: The rendered DAG
        :rtype: DAG
        """
        dag = DAG(
            dag_id=self.entity.task_id,
            default_args={
                "owner": "airflow",
                "start_date": datetime(2019, 1, 1),
            },
            schedule_interval=None,
        )

        for task in self.group.tasks:
            # import the operator class
            module_name, class_name = task.operator_class.rsplit(".", 1)
            module = importlib.import_module(module_name)
            operator = getattr(module, class_name)

            # instantiate the operator
            t = operator(**task.arguments)

            for upstream_task_id in task.upstream_task_ids:
                t.set_upstream(upstream_task_id)

            t.set_downstream(task.task_id)

        return dag
