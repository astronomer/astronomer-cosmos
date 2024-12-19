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


In addition to Scarf's default data collection, DAG Factory collects the following information:

- Cosmos version
- Airflow version
- Python version
- Operating system & machine architecture
- Event type
- DAG that uses Cosmos hash
- Total tasks in DAGs that use Cosmos
- Total Cosmos tasks
- Total Cosmos task groups

No user-identifiable information (IP included) is stored in Scarf.
