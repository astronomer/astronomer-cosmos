.. _cosmos-managed-venv:

Cosmos-managed virtual environment execution mode
========================================================

The ``virtualenv`` mode runs dbt commands from Python virtual environments created and managed by Cosmos. This mode removes the need to create a virtual environment at build time, unlike ``ExecutionMode.LOCAL``, while avoiding package conflicts. It is intended for cases where:

- You can't install dbt directly in the Airflow environment, either in the same environment or a dedicated one.
- Multiple dbt installations are required, and you prefer Cosmos to manage them without modifying the Airflow deployment.
- Speed is not a concern, and you can afford for Cosmos to create and update the Python virtual environment during the execution of each dbt node.

In most cases, the local execution mode with ``ExecutionConfig.dbt_executable_path`` is the preferred option, as it allows you to manage the dbt environment during the Airflow deployment process, instead of per-dbt node execution.

When you use ``virtualenv`` mode, you are responsible for declaring which version of ``dbt`` to use by giving the argument ``py_requirements``. Set this argument directly in operator instances or when you instantiate ``DbtDag`` and ``DbtTaskGroup`` as part of ``operator_args``.

Similar to the ``local`` execution mode, Cosmos converts Airflow Connections into a way ``dbt`` understands them by creating a ``dbt`` profile file (``profiles.yml``).
Also similar to the ``local`` execution mode, Cosmos will by default attempt to use a ``partial_parse.msgpack`` if one exists to speed up parsing.

Some drawbacks of the ``virtualenv`` approach:

- It is slower than ``local`` because it may create and update a new Python virtual environment for each Cosmos dbt task run, depending on the Airflow executor and if you set the ``ExecutionConfig.virtualenv_dir`` configuration.
- If dbt is unavailable in the Airflow scheduler, the default ``LoadMode.DBT_LS`` will not work. In this scenario, you must use a :ref:`parsing-methods` that does not rely on dbt, such as ``LoadMode.MANIFEST``.
- Only ``InvocationMode.SUBPROCESS`` is supported currently, attempt to use ``InvocationMode.DBT_RUNNER`` will raise error.

Example of how to use:

.. literalinclude:: ../../../../dev/dags/example_virtualenv.py
   :language: python
   :start-after: [START virtualenv_example]
   :end-before: [END virtualenv_example]
