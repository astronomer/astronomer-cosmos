Configuration
================

Cosmos offers a few different configuration options for how your dbt project is run and structured. This page describes the available options and how to configure them.

Testing
----------------------

By default, Cosmos will add a test after each model. This can be overridden using the ``test_behavior`` field. The options are:

- ``after_each`` (default): turns each model into a task group with two steps: run the model, and run the tests
- ``after_all``: each model becomes a single task, and the tests only run if all models are run successfully
- ``none``: don't include tests

Example:

.. code-block:: python

    from cosmos import DbtTaskGroup

    jaffle_shop = DbtTaskGroup(
        # ...
        test_behavior="snowflake_default",
    )


Warn Notification
----------------------
.. note::

    As of now, this feature is only available for the default execution mode ``local``

Cosmos enables you to receive warning notifications from tests and process them using a callback function.
The ``on_warning_callback`` parameter adds two extra context variables to the callback function: ``test_names`` and ``test_results``.
``test_names`` contains the names of the tests that generated a warning, while ``test_results`` holds the corresponding test results
at the same index. Both the ``test_names`` and ``test_results`` variables are lists of strings.

For example, the following code snippet shows how to send a Slack message when a warning occurs:

.. code-block:: python

    from cosmos import DbtDag
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    from airflow.utils.context import Context


    def warning_callback_func(context: Context):
        tests = context.get("test_names")
        results = context.get("test_results")

        warning_msgs = ""
        for test, result in zip(tests, results):
            warning_msg = f"""
            *Test*: {test}
            *Result*: {result}
            """
            warning_msgs += warning_msg

        if warning_msgs:
            slack_msg = f"""
            :large_yellow_circle: Airflow-DBT task with WARN.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            {warning_msgs}
            """

            slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_conn_id")
            slack_hook.send(text=slack_msg)


    mrr_playbook = DbtDag(
        # ...
        on_warning_callback=warning_callback_func,
    )

When at least one WARN message is present, the function passed to ``on_warning_callback`` will be triggered. In the example above, the following message will be sent to Slack:

.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/callback_slack.png
   :width: 600

.. note::

    If warnings that are not associated with tests occur (e.g. freshness warnings), they will still trigger the
    ``on_warning_callback`` method above. However, these warnings will not be included in the ``test_names`` and
    ``test_results`` context variables, which are specific to test-related warnings.

Selecting and Excluding
----------------------

Cosmos allows you to filter by configs (e.g. ``materialized``, ``tags``) using the ``select`` and ``exclude`` parameters. If a model contains any of the configs in the ``select``, it gets included as part of the DAG/Task Group. Similarly, if a model contains any of the configs in the ``exclude``, it gets excluded from the DAG/Task Group.

The ``select`` and ``exclude`` parameters are dictionaries with the following keys:

- ``configs``: a list of configs to filter by. The configs are in the format ``key:value``. For example, ``tags:daily`` or ``materialized:table``.
- ``paths``: a list of paths to filter by. The paths are in the format ``path/to/dir``. For example, ``analytics`` or ``analytics/tables``.

.. note::
    Cosmos currently reads from (1) config calls in the model code and (2) .yml files in the models directory for tags. It does not read from the dbt_project.yml file.

Examples:

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        # ...
        select={"configs": ["tags:daily"]},
    )

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        # ...
        select={"configs": ["schema:prod"]},
    )

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        # ...
        select={"paths": ["analytics/tables"]},
    )


Viewing Compiled SQL
----------------------

When using the local execution mode, Cosmos will store the compiled SQL for each model in the ``compiled_sql`` field of the task's ``template_fields``. This allows you to view the compiled SQL in the Airflow UI.

If you'd like to disable this feature, you can set ``should_store_compiled_sql=False`` on the local operator (or via the ``operator_args`` parameter on the DAG/Task Group).
