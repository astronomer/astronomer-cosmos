.. _use-profile-mapping:

Use a profile mapping
=====================

Profile mappings are utilities provided by Cosmos that translate `Apache Airflow® <https://airflow.apache.org/>`_ connections to dbt profiles. This means that
you can use the same connection objects you use in Airflow to authenticate with your database in dbt. To do so, there's
a class in Cosmos for each Airflow connection to dbt profile mapping.

.. note::
   Cosmos natively supports all adapters compatible with **dbt Core** and **dbt Fusion** without any reliance on dbt Platform. This includes both official `Trusted Adapters <https://docs.getdbt.com/docs/trusted-adapters>`_ and `Community Adapters <https://docs.getdbt.com/docs/community-adapters>`_.
   While some of these adapters lack a built-in automated profile mapping class in Cosmos, including
   `AWS Glue <https://docs.getdbt.com/docs/local/connect-data-platform/glue-setup?version=2.0&name=Fusion>`_,
   `Dremio <https://docs.getdbt.com/docs/local/connect-data-platform/dremio-setup?version=2.0&name=Fusion>`_,
   `Azure Synapse <https://docs.getdbt.com/docs/local/connect-data-platform/azuresynapse-setup?version=2.0&name=Fusion>`_, and
   `IBM Netezza <https://docs.getdbt.com/docs/local/connect-data-platform/ibmnetezza-setup?version=2.0&name=Fusion>`_, among others,
   they remain fully operational out of the box. In these scenarios, you can simply bypass automated connection mappings entirely and pass a user-provided ``profiles.yml`` file configuration instead.
   For a detailed implementation guide, see :ref:`use-your-profiles-yml`.

You can find the available profile mappings on the left-hand side of this page. Each profile mapping is imported from
``cosmos.profiles`` and takes two arguments:

- ``conn_id``: the Airflow connection ID to use.
- ``profile_args``: a dictionary of additional arguments to pass to the dbt profile. This is useful for specifying
  values that are not in the Airflow connection. This also acts as an override for any values that are in the Airflow
  connection but should be overridden.

Below is an example of using the Snowflake profile mapping, where we take most arguments from the Airflow connection
but override the ``database`` and ``schema`` values:

.. code-block:: python

    from cosmos.profiles import SnowflakeUserPasswordProfileMapping

    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="my_snowflake_conn_id",
            profile_args={
                "database": "my_snowflake_database",
                "schema": "my_snowflake_schema",
            },
        ),
    )

    dag = DbtDag(profile_config=profile_config, ...)

Note that when using a profile mapping, the profiles.yml file gets generated with the profile name and target name
you specify in ``ProfileConfig``.

Choose the connection when the task runs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 1.17.0

With ``ExecutionMode.LOCAL``, ``VIRTUALENV``, ``WATCHER`` and ``AIRFLOW_ASYNC``, ``conn_id`` accepts a Jinja template, which Cosmos renders when each task runs. In this example, it comes from the DAG param ``conn_id``, set with ``params={"conn_id": "example_conn"}``:

.. literalinclude:: ../../../dev/dags/example_model_version.py
    :language: python
    :start-after: [START templated_conn_id]
    :end-before: [END templated_conn_id]

Cosmos cannot render the template while parsing the DAG, so the connection must not be needed then. This is the case with the default mock profile and with ``LoadMode.DBT_MANIFEST``. With ``LoadMode.DBT_LS`` and ``RenderConfig(enable_mock_profile=False)``, or with ``get_automatic_profile_mapping``, Cosmos raises an error.

A per-node ``conn_id`` set in :ref:`profile-customise-per-node` takes precedence.
