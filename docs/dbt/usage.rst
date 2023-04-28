Usage
======

Cosmos supports two standard way of rendering dbt projects: either as a full DAG or as a Task Group.

By default, Cosmos will look in the ``/usr/local/airflow/dbt`` directory (next to the ``dags`` folder if you're using the `Astro CLI <https://github.com/astronomer/astro-cli>`_). You can override this using the ``dbt_root_path`` argument in either :class:`cosmos.providers.dbt.DbtDag` or :class:`cosmos.providers.dbt.DbtTaskGroup`. You can also override the default models directory, which is ``"models"`` by default, using the ``dbt_models_dir`` argument.

Rendering
+++++++++

Full DAG
--------

The :class:`cosmos.providers.dbt.DbtDag` class can be used to render a full DAG for a dbt project. This is useful if you want to run all of the dbt models in a project as a single DAG.

.. code-block:: python

    from cosmos.providers.dbt import DbtDag

    jaffle_shop = DbtDag(
        dbt_project_name="jaffle_shop",
        conn_id="airflow_db",
        dbt_args={"schema": "public"},
        dag_id="attribution-playbook",
        start_date=datetime(2022, 11, 27),
        schedule_interval="@daily",
    )


Task Group
----------

The :class:`cosmos.providers.dbt.DbtTaskGroup` class can be used to render a task group for a dbt project. This is useful if you want to run your dbt models in a project as a single task group, and include other non-dbt tasks in your DAG (e.g., extracting and loading data).

.. code-block:: python

    from pendulum import datetime

    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from cosmos.providers.dbt.task_group import DbtTaskGroup


    with DAG(
        dag_id="extract_dag",
        start_date=datetime(2022, 11, 27),
        schedule="@daily",
    ) as dag:

        e1 = EmptyOperator(task_id="ingestion_workflow")

        dbt_tg = DbtTaskGroup(
            group_id="dbt_tg",
            dbt_project_name="jaffle_shop",
            conn_id="airflow_db",
            dbt_args={
                "schema": "public",
            },
        )

        e2 = EmptyOperator(task_id="some_extraction")

        e1 >> dbt_tg >> e2


Connections & profiles
+++++++++++++++++++++++++++++++

Cosmos currently supports the following connection types.

* bigquery
* databricks
* exasol
* postgres
* redshift
* snowflake
* spark

For specific details on the different connections please see below.

Cosmos' dbt integration uses Airflow connections to connect to your data sources.
You can use the Airflow UI to create connections for your data sources.
For more information, see the `Airflow documentation <https://airflow.apache.org/docs/apache-airflow/stable/howto/connection/index.html>`__.

This means you don't need to manage a separate profiles.yml file for your dbt project.
Instead, you manage all your connections in one place and can leverage Airflow's connections model (e.g. to use a secrets backend).

To use your Airflow connections, you need to pass in the ``conn_id``.
If you'd like to override the database or schema within your Airflow connection then you can do this via dbt_args.

Under the hood, this information gets translated to a profiles.yml file (using environment variables, not written to the disk) that dbt uses to connect to your data source.

For more information, see the `dbt documentation <https://docs.getdbt.com/reference/dbt-jinja-functions/env_var>`__.

.. code-block:: python

    from cosmos.providers.dbt import DbtDag

    jaffle_shop = DbtDag(
        # ...
        conn_id="airflow_db",
        dbt_args={
            "db_name": "my_db",  # overrides any database stored on the connection,
            "schema": "public",  # overrides any schema stored on the connection.
        },
        # ...
    )

BigQuery
---------

Cosmos supports the keyfile json `method <https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#service-account-json>`__
which is common between Airflow and dbt.

Databricks
-----------

If you need to reference a Unity Catalog then pass the name of the catalog in db_name within dbt_args.

.. code-block:: python

    from cosmos.providers.dbt import DbtTaskGroup

    tg = DbtTaskGroup(
        # ...
        dbt_args = {
            # ...
            'db_name': 'unity-catalog'
        }
        # ...
    )

Postgres
---------

The database name is determined in the following order.

#. db_name from dbt_args.
#. schema from dbt_args.
#. schema from Airflow connection.

Neither dbt nor psycopg2 allows for a schema argument to be provided during connection.

Schema was renamed to Database in the Airflow hook with this `pull request <https://github.com/apache/airflow/pull/26744>`__
to prevent confusion.
In the connection it remains as schema, even though it refers to a database.

Redshift
---------

Builds upon Postgres so the details on schemas are the same.

Cosmos supports the password based authentication `method <https://docs.getdbt.com/reference/warehouse-setups/redshift-setup#password-based-authentication>`__

Exasol
---------

Builds upon Postgres so the details on schemas are the same.

Cosmos supports the password based authentication `method <https://docs.getdbt.com/reference/warehouse-setups/exasol-setup>`__


Snowflake
----------

Cosmos supports the dbt User/Password authentication `method <https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup#user--password-authentication>`__

Supports both pre and post apache-airflow-providers-snowflake v4.0.2 changes for Snowflake extra arguments implemented
by this `pull request <https://github.com/apache/airflow/pull/26764>`__.

Trino
______

Cosmos supports ldap, certificate, jwt and kerberos authentication which are common between dbt and Airflow.

Kerberos authentication does not support keytabs so a password is required instead.

To provide a default Trino catalog this should come from extras in the Airflow connection or it can be provided/overriden
using `dbt_args = {"db_name": "my_catalog"}}`

To provide a default Trino schema a.k.a database this will come from the Airflow connection schema or it can be provided/overriden
using `dbt_args = {"schema": "my_schema"}}`

Spark Thrift
______

Spark Thrift uses Airflow Spark JDBC connection.

The schema name is determined in the following order.

#. schema from dbt_args.
#. schema from Airflow connection.

schema and database is interchangeable in dbt-spark.
