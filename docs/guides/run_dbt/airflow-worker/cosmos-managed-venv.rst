.. _cosmos-managed-venv:

Cosmos-managed virtual environment execution mode
========================================================

If you're using managed Airflow, we recommend you use the ``virtualenv`` execution mode.

The ``virtualenv`` mode isolates the Airflow worker dependencies from ``dbt`` by managing a Python virtual environment created during task execution and deleted afterwards.

In this case, you are responsible for declaring which version of ``dbt`` to use by giving the argument ``py_requirements``. Set this argument directly in operator instances or when you instantiate ``DbtDag`` and ``DbtTaskGroup`` as part of ``operator_args``.

Similar to the ``local`` execution mode, Cosmos converts Airflow Connections into a way ``dbt`` understands them by creating a ``dbt`` profile file (``profiles.yml``).
Also similar to the ``local`` execution mode, Cosmos will by default attempt to use a ``partial_parse.msgpack`` if one exists to speed up parsing.

Some drawbacks of this approach:

- It is slower than ``local`` because it creates a new Python virtual environment for each Cosmos dbt task run.
- If dbt is unavailable in the Airflow scheduler, the default ``LoadMode.DBT_LS`` will not work. In this scenario, you must use a :ref:`parsing-methods` that does not rely on dbt, such as ``LoadMode.MANIFEST``.
- Only ``InvocationMode.SUBPROCESS`` is supported currently, attempt to use ``InvocationMode.DBT_RUNNER`` will raise error.

Example of how to use:

.. literalinclude:: ../../../../dev/dags/example_virtualenv.py
   :language: python
   :start-after: [START virtualenv_example]
   :end-before: [END virtualenv_example]