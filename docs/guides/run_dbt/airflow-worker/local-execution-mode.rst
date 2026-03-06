.. _local-execution:

Local execution mode
====================

By default, Cosmos uses the ``local`` execution mode.

The ``local`` execution mode is the fastest way to run Cosmos operators since they don't neither install ``dbt`` nor build docker containers. If you use managed Airflow services such as
Google Cloud Composer, you might want to use a different execution mode, since Airflow and ``dbt`` dependencies can conflict (:ref:`execution-modes-local-conflicts`). On an managed Airflow service, you you might not be able to install ``dbt`` in a custom path.

The ``local`` execution mode assumes that the Airflow worker node can access a ``dbt`` binary.

If ``dbt`` was not installed as part of the Cosmos packages, you can define a custom path to ``dbt`` by declaring the argument ``dbt_executable_path``.

.. note::
    Starting in the 1.4 version, Cosmos tries to leverage the dbt partial parsing (``partial_parse.msgpack``) to speed up task execution.
    This feature is bound to `dbt partial parsing limitations <https://docs.getdbt.com/reference/parsing#known-limitations>`_.
    Learn more: :ref:`partial-parsing`.

When using the ``local`` execution mode, Cosmos converts Airflow Connections into a native ``dbt`` profiles file (``profiles.yml``).

Example of how to use, for instance, when ``dbt`` was installed together with Cosmos:

.. literalinclude:: ../../../../dev/dags/basic_cosmos_dag.py
    :language: python
    :start-after: [START local_example]
    :end-before: [END local_example]
