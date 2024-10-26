.. _task-display-name:

Task display name
================

.. note::
    This feature is only available for Airflow >= 2.9.

In Airflow, `task_id` does not support non-ASCII characters. Therefore, if users wish to use non-ASCII characters (such as their native language) as display names while keeping `task_id` in ASCII, they can use the `display_name` parameter.

To work with projects that use non-ASCII characters in model names, the `set_task_id_by_node` field of `RenderConfig` can be utilized.

Example:

You can provide a function to convert the model name to an ASCII-compatible format. The function’s output is used as the TaskID, while the display name on Airflow remains as the original model name.

.. code-block:: python

    from slugify import slugify

    def set_task_id_by_node(node):
        return slugify(node.name)

    from cosmos import DbtTaskGroup, RenderConfig

    jaffle_shop = DbtTaskGroup(
        render_config=RenderConfig(
        set_task_id_by_node=set_task_id_by_node
        )
    )
