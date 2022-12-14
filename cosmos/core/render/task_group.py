import importlib

from airflow.utils.task_group import TaskGroup

from cosmos.core.graph.group import Group


class CosmosTaskGroup:
    """
    Render a Task or Group as an Airflow TaskGroup.
    """

    def render(self, group: Group) -> TaskGroup:
        """
        Render the TaskGroup.

        :return: The rendered TaskGroup
        :rtype: TaskGroup
        """

        @task_group(group_id=group.group_id)
        def generate_task_group(self, group: Group) -> TaskGroup:
            """
            Generate the TaskGroup.

            :return: The generated TaskGroup
            :rtype: TaskGroup
            """
            task_group = TaskGroup(group_id=group.group_id)

            with task_group:
                for task in group.tasks:
                    # import the operator class
                    module_name, class_name = task.operator_class.rsplit(".", 1)
                    module = importlib.import_module(module_name)
                    operator = getattr(module, class_name)

                    # instantiate the operator
                    t = operator(**task.arguments)

                    for upstream_task_id in task.upstream_task_ids:
                        t.set_upstream(upstream_task_id)

                    t.set_downstream(task.task_id)
                    t()

        return generate_task_group(self, group)
