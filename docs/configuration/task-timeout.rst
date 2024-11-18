.. _task-timeout:

Task Timeout
================

In Airflow, the "execution_timeout" parameter allows you to set the maximum runtime for a Task.
With Cosmos, you can specify an execution_timeout for each dbt model converted to a Task.
This lets users set a threshold for the maximum runtime of a model, triggering a timeout error if the execution exceeds this limit.

By adding cosmos_task_timeout to the config of a dbt model, Cosmos will automatically apply the specified timeout to the Task based on this value.

Example:

.. code-block:: yaml

    version: 2

    models:
    - name: my_model
      config:
        - cosmos_task_timeout: 600 # Specify in seconds.
