.. _task-display-name:

Task display name
================

.. note::
    This feature is only available for Airflow >= 2.9.

In Airflow, ``task_id`` does not support non-ASCII characters. Therefore, if users wish to use non-ASCII characters (such as their native language) as display names while keeping ``task_id`` in ASCII, they can use the ``display_name`` parameter.

To work with projects that use non-ASCII characters in model names, the ``normalize_task_id`` field of ``RenderConfig`` can be utilized.

Example:

You can provide a function to convert the model name to an ASCII-compatible format. The function’s output is used as the TaskID, while the display name on Airflow remains as the original model name.

.. code-block:: python

    from slugify import slugify


    def normalize_task_id(node):
        return slugify(node.name)


    from cosmos import DbtTaskGroup, RenderConfig

    jaffle_shop = DbtTaskGroup(
        render_config=RenderConfig(normalize_task_id=normalize_task_id)
    )

.. note::
    Although the slugify example often works, it may not be suitable for use in actual production. Since slugify performs conversions based on pronunciation, there may be cases where task_id is not unique due to homophones and similar issues.

There may be cases where the user might want to preserve the ``task_id`` while altering the ``display_name`` only. This can be accomplished with the ``normalize_task_display_name`` field of ``RenderConfig``.

Example:

You can provide a function to display the dbt models in airflow UI without any suffix (``_source``, ``_run``, etc)

.. code-block:: python
    def normalize_task_display_name(node):
        return f"{node.name}"


    from cosmos import DbtTaskGroup, RenderConfig

    jaffle_shop = DbtTaskGroup(
        render_config=RenderConfig(normalize_task_display_name=normalize_task_display_name)
    )
..
