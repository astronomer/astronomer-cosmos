.. _compiled-sql:

Compiled SQL
------------

When using the local execution mode, Cosmos can store the compiled SQL for each model in the ``compiled_sql`` field of the task's ``template_fields``. This allows you to view the compiled SQL in the Airflow UI.

On **Airflow 2.x**, this feature is **enabled by default** (``should_store_compiled_sql=True``).

On **Airflow 3+**, this feature is **disabled by default** (``should_store_compiled_sql=False``).
This is because Airflow 3 rotates the task instance ID on retry, which causes a race condition
when Cosmos writes the rendered template fields after execution: the original task instance ID
no longer exists by the time the write occurs, resulting in ``Task Instance not found`` (HTTP 404) errors.
While these errors are harmless (the task still runs correctly), they produce noisy log output. To avoid
this, the feature is disabled by default on Airflow 3.

If you'd like to explicitly enable or disable this feature, you can set ``should_store_compiled_sql`` on the local operator (or via the ``operator_args`` parameter on the DAG/Task Group). For example:

.. code-block:: python

    from cosmos import DbtDag

    DbtDag(
        operator_args={"should_store_compiled_sql": False},
        # ...,
    )
