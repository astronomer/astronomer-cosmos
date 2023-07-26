.. _compiled-sql:

Compiled SQL
====================

When using the local execution mode, Cosmos will store the compiled SQL for each model in the ``compiled_sql`` field of the task's ``template_fields``. This allows you to view the compiled SQL in the Airflow UI.

If you'd like to disable this feature, you can set ``should_store_compiled_sql=False`` on the local operator (or via the ``operator_args`` parameter on the DAG/Task Group). For example:

.. code-block:: python

    from cosmos import DbtDag

    DbtDag(
        operator_args={
            "should_store_compiled_sql": False
        },
        # ...,
    )
