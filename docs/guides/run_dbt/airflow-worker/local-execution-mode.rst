.. _local-execution:

Local execution mode
--------------------

By default, Cosmos uses the ``local`` execution mode. It is the fastest way to run Cosmos operators, since it runs dbt either as a library or as a local subprocess.
For situations where dbt and `Apache Airflow® <https://airflow.apache.org/>`_ dependencies conflict, :ref:`execution-modes-local-conflicts`, you most likely have the option to pre-install dbt in an isolated Python virtual environment, either as part of the container image or as part of a pre-start script.

The ``local`` execution mode assumes that the Airflow worker node can access a ``dbt`` binary. If ``dbt`` was not installed alongside Cosmos, you can create a dedicated virtual environment and define a custom path to ``dbt`` by declaring the argument ``ExecutionConfig.dbt_executable_path``.

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
