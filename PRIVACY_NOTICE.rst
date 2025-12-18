Privacy Notice
==============

This project follows the `Privacy Policy of Astronomer <https://www.astronomer.io/privacy/>`_.

Collection of Data
------------------

Astronomer Cosmos integrates `Scarf <https://about.scarf.sh/>`_ to collect basic telemetry data during operation.
This data assists the project maintainers in better understanding how Cosmos is used.
Insights gained from this telemetry are critical for prioritizing patches, minor releases, and
security fixes. Additionally, this information supports key decisions related to the development road map.

Deployments and individual users can opt-out of analytics by setting the configuration:


.. code-block::

    [cosmos] enable_telemetry False


As described in the `official documentation <https://docs.scarf.sh/gateway/#do-not-track>`_, it is also possible to opt out by setting one of the following environment variables:

.. code-block::

    DO_NOT_TRACK=True
    SCARF_NO_ANALYTICS=True


In addition to Scarf's default data collection, Cosmos collect the following information when running Cosmos-powered DAGs:

- Cosmos version
- Airflow version
- Python version
- Operating system & machine architecture
- Event type
- The DAG hash
- Total tasks
- Total Cosmos tasks

When running **Cosmos-powered tasks**, the following information is collected:

- Operator name
- Cosmos Execution mode
- Cosmos Invocation method
- dbt command executed
- Whether the task is a subclass of a Cosmos-defined class
- Whether the task has a callback configured
- Whether the task is an Airflow-mapped task
- Whether ``dbt deps`` was run as part of the task execution
- Task status
- Task duration
- Cosmos version
- Airflow version
- Python version
- Operating system and machine architecture
- Whether profile mapping or YAML file is used
- Profile mapping class
- Data warehouse

No user-identifiable information (IP included) is stored in Scarf.
