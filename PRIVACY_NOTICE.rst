Privacy Notice
==============

By default, telemetry is disabled for Astronomer customers running recent Astro Runtime images or Astro Private Cloud — see `Telemetry on Astronomer`_ for the specific versions.

This project follows the `Privacy Policy of Astronomer <https://www.astronomer.io/privacy/>`_.

Telemetry on Astronomer
-----------------------

Since May 2025, `Astro Runtime <https://www.astronomer.io/docs/runtime/runtime-release-notes>`_ images set the
environment variable ``SCARF_NO_ANALYTICS=True``, which disables Cosmos telemetry by default:

- Airflow 3-based images: Astro Runtime 3.0-2 and newer
- Airflow 2-based images: Astro Runtime 11.18.0, 12.9.0, 13.0.0 and newer

`Astro Private Cloud <https://www.astronomer.io/docs/astro-private-cloud/>`_ (APC) also disables telemetry by
default, setting both ``SCARF_NO_ANALYTICS=True`` and ``DO_NOT_TRACK=True`` in all Deployments, regardless of
the Astro Runtime version.

Collection of Data
------------------

Astronomer Cosmos integrates `Scarf <https://about.scarf.sh/>`_ to collect basic telemetry data during operation.
This data is collected and processed by Scarf in accordance with the `Scarf Privacy Policy <https://about.scarf.sh/privacy-policy/>`_.
It assists the project maintainers in better understanding how Cosmos is used.
Insights gained from this telemetry are critical for prioritizing patches, minor releases, and
security fixes. Additionally, this information supports key decisions related to the development roadmap.

Deployments and individual users can opt out of analytics by setting the configuration:

.. code-block:: ini

    [cosmos]
    enable_telemetry = false

or the equivalent environment variable:

.. code-block:: bash

    AIRFLOW__COSMOS__ENABLE_TELEMETRY=false

As described in the `Scarf documentation <https://docs.scarf.sh/gateway/#do-not-track>`_, it is also possible to opt out by setting one of the following environment variables (values are case-insensitive):

.. code-block:: bash

    DO_NOT_TRACK=true
    SCARF_NO_ANALYTICS=true


In addition to Scarf's default data collection, Cosmos collects the following information when running Cosmos-powered DAGs:

- Cosmos version
- Airflow version
- Python version
- Operating system & machine architecture
- Event type
- The DAG hash
- Total tasks
- Total Cosmos tasks
- Whether automatic load mode was specified
- Actual load mode used (dbt_ls, dbt_manifest, custom, etc.)
- Invocation mode (subprocess or dbt_runner)
- Whether dbt deps installation is enabled
- Whether custom node converters are used
- Test behavior (after_each, after_all, none, etc.)
- Source behavior (how dbt sources are rendered)
- Total number of dbt models in project
- Number of dbt models selected for rendering

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

When the **dbt docs plugin** is accessed, the following information is collected:

- Storage type (s3, gcs, azure, http, local, or not_configured)
- Whether the docs directory is configured
- Whether a custom connection ID is used
- Whether a custom project name is set (Airflow 3 only)

Astronomer does not track user-identifiable information through Cosmos telemetry.
For details on how Scarf handles the collected data, please refer to the
`Scarf Privacy Policy <https://about.scarf.sh/privacy-policy/>`__.
