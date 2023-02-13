Connections
===========

Cosmos supports these common dbt core adapters. However, unlike running dbt core from the CLI, Cosmos does *not* need a
profiles.yml file to authenticate. Instead, you will create an Airflow connection object for your Database and pass it
to the desired Cosmos component.

.. tabs::

   .. tab:: Snowflake

        .. note::
            Cosmos currently only supports User/Password authentication to Snowflake. Additional authentication methods will be
            added soon. If there is an authentication method that your organization requires, open a PR/Issue on the Cosmos GitHub
            repo `here <https://github.com/astronomer/astronomer-cosmos>`_.

        Here are the specific steps that you should take if you are configuring Cosmos to run against Snowflake:

        1. Add Cosmos and Snowflake Provider to your `requirements.txt`

            .. code-block:: text

                astronomer-cosmos[dbt-snowflake]
                apache-airflow-providers-snowflake

        2. Create an Airflow Connection for Snowflake. Fore more information on creating the Snowflake Airflow Connection, see
        Airflow's official documentation `here <https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html>`_.
        In the Airflow UI, navigate to `Admin >> Connections` and create a new connection like this:

            .. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/connections_doc/snowflake_airflow_connection.png
               :width: 800

               Creating a snowflake connection in Airflow

        (Optional) If your team prefers storing connections as environment variables or in a supported secrets backend, the connection URI
        for this example would look like this:

            .. note::
                The following variable value would need to be encoded. You can use a service like `urlencoder.org <https://www.urlencoder.org/>`_  to do that.

            .. code-block:: text

                AIRFLOW_CONN_SNOWFLAKE_DEFAULT=snowflake://user123:p@ssword!@/my_db_schema?__extra__={"account":+"gp54783",+"warehouse":+"my_warehouse",+"database":+"my_db",+"region":+"us-east-1",+"role":+"user123",+"insecure_mode":+false}

        3. With the dependencies and connection in place, you can pass this connection to a DbtTaskGroup, DbtDag, or Dbt Operator
        Class via the `conn_id` parameter:

            .. code-block:: python

                jaffle_shop = DbtTaskGroup(
                    ...
                    conn_id="snowflake_default",
                )

        .. note::
            If you left the `Schema` field blank when configuring your Snowflake connection or if you would like to override it,
            then specify the schema parameter via the `dbt_args`:

            .. code-block:: python

                jaffle_shop = DbtTaskGroup(
                    conn_id="snowflake_default",
                    dbt_args={
                        "schema": "my_schema",
                    }
                )


   .. tab:: Redshift

        .. note::
            Cosmos currently only supports User/Password authentication to Redshift. Additional authentication methods will be
            added soon. If there is an authentication method that your organization requires, open a PR/Issue on the Cosmos GitHub
            repo `here <https://github.com/astronomer/astronomer-cosmos>`_.

        Here are the specific steps that you should take if you are configuring Cosmos to run against Redshift:

        1. Add Cosmos and the Amazon Provider to your `requirements.txt`

            .. code-block:: text

                astronomer-cosmos[dbt-redshift]
                apache-airflow-providers-amazon

        2. Create an Airflow Connection for Redshift. Fore more information on creating the Redshift Airflow Connection, see
        Airflow's official documentation `here <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html>`_.
        In the Airflow UI, navigate to `Admin >> Connections` and create a new connection like this:

            .. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/connections_doc/redshift_airflow_connection.png
               :width: 800

               Creating a redshift connection in Airflow

        (Optional) If your team prefers storing connections as environment variables or in a supported secrets backend, the connection URI
        for this example would look like this:

            .. code-block:: text

                AIRFLOW_CONN_REDSHIFT_DEFAULT=redshift://username:password@redshift-cluster.endpoint.us-west-2.redshift.amazonaws.com:5439/db_name

        3. With the dependencies and connection in place, you can pass this connection to a DbtTaskGroup, DbtDag, or Dbt Operator
        Class via the `conn_id` parameter:

        .. note::
            Since the Airflow Connections for redshift do not provide a field for a default schema parameter, you will need to
            add it via the `dbt_args`:

        .. code-block:: python

            jaffle_shop = DbtTaskGroup(
                conn_id="redshift_default",
                dbt_args={
                    "schema": "my_schema",
                }
            )

   .. tab:: BigQuery

        .. note::
            Cosmos currently only supports Service Account JSON method when authenticating to BigQuery. Additional
            authentication methods will be added soon. If there is an authentication method that your organization requires, open a
            PR/Issue on the Cosmos GitHub repo `here <https://github.com/astronomer/astronomer-cosmos>`_.

        Here are the specific steps that you should take if you are configuring Cosmos to run against BigQuery:

        1. Add Cosmos and the Google Provider to your `requirements.txt`

            .. code-block:: text

                astronomer-cosmos[dbt-bigquery]
                apache-airflow-providers-google

        2. Create an Airflow Connection for BigQuery. Fore more information on creating the Redshift Airflow Connection, see
        Airflow's official documentation `here <https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html>`_.
        In the Airflow UI, navigate to `Admin >> Connections` and create a new connection like this (note that the `Keyfile
        JSON` parameter simply contains the raw contents of the JSON file for the service account on GCP):

            .. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/connections_doc/bigquery_airflow_connection.png
               :width: 800

               Creating a BigQuery connection in Airflow

        (Optional) If your team prefers storing connections as environment variables or in a supported secrets backend, the connection URI
        for this example would look like this:

            .. note::
                The following variable value would need to be encoded. You can use a service like `urlencoder.org <https://www.urlencoder.org/>`_  to do that.

            .. code-block:: text

                AIRFLOW_CONN_BIGQUERY_DEFAULT=google-cloud-platform:///?__extra__={"keyfile_dict":+"{+++\"type\":+\"service_account\",+++\"project_id\":+\"your-gcp-project-id\",+++\"private_key_id\":+\"your-gcp-private-key-id\",+++\"private_key\":+\"-----BEGIN+PRIVATE+KEY-----\\nyour-gcp-private-key\\n-----END+PRIVATE+KEY-----\\n\",+++\"client_email\":+\"service_account_email@your-gcp-project-id.iam.gserviceaccount.com\",+++\"client_id\":+\"your-client-id\",+++\"auth_uri\":+\"https://accounts.google.com/o/oauth2/auth\",+++\"token_uri\":+\"https://oauth2.googleapis.com/token\",+++\"auth_provider_x509_cert_url\":+\"https://www.googleapis.com/oauth2/v1/certs\",+++\"client_x509_cert_url\":+\"https://www.googleapis.com/robot/v1/metadata/x509/service_account_email%40your-gcp-project-id.iam.gserviceaccount.com\"+}",+"num_retries":+5}

        3. With the dependencies and connection in place, you can pass this connection to a DbtTaskGroup, DbtDag, or Dbt Operator
        Class via the `conn_id` parameter:

        .. note::
            Since the Airflow Connections for BigQuery do not provide a field for a default dataset parameter, you will need to
            add it via the `dbt_args`:

        .. code-block:: python

            jaffle_shop = DbtTaskGroup(
                conn_id="bigquery_default",
                dbt_args={
                    "schema": "your_bigquery_dataset",
                }
            )

   .. tab:: Databricks

        Follow these steps when configuring Cosmos to run against Databricks:

        1. Add Cosmos and the Databricks Provider to your `requirements.txt`

            .. code-block:: text

                astronomer-cosmos[dbt-databricks]
                apache-airflow-providers-databricks

        2. Create an Airflow Connection for Databricks. Fore more information on creating the Databricks Airflow Connection, see
        Airflow's official documentation `here <https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html>`_.
        In the Airflow UI, navigate to `Admin >> Connections` and create a new connection like this:

            .. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/connections_doc/databricks_airflow_connection.png
               :width: 800

               Creating a databricks connection in Airflow

        (Optional) If your team prefers storing connections as environment variables or in a supported secrets backend, the connection URI
        for this example would look like this:

            .. note::
                The following variable value would need to be encoded. You can use a service like `urlencoder.org <https://www.urlencoder.org/>`_ to do that.

            .. code-block:: text

                AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks://adb-1234567891011.12.azuredatabricks.net/your_databricks_catalog?http_path=/sql/protocolv1/o/1234567891011/0503-58462-kdw76lbv&token=<your-databricks-token>

        3. With the dependencies and connection in place, you can pass this connection to a DbtTaskGroup, DbtDag, or Dbt Operator
        Class via the `conn_id` parameter:

        .. note::
            Since the Airflow Connections for databricks do not provide a field for a default schema parameter, you will need to
            add it via the `dbt_args`:

        .. code-block:: python

            jaffle_shop = DbtTaskGroup(
                conn_id="databricks_default",
                dbt_args={
                    "schema": "your_databricks_db",
                }
            )

   .. tab:: Postgres

        Here are the specific steps that you should take if you are configuring Cosmos to run against Postgres:

        1. Add Cosmos and the Postgres Provider to your `requirements.txt`

            .. code-block:: text

                astronomer-cosmos[dbt-postgres]
                apache-airflow-providers-postgres

        2. Create an Airflow Connection for Postgres. Fore more information on creating the Postgres Airflow Connection, see
        Airflow's official documentation `here <https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html>`_.
        In the Airflow UI, navigate to `Admin >> Connections` and create a new connection like this:

            .. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/connections_doc/postgres_airflow_connection.png
               :width: 800

               Creating a postgres connection in Airflow

        (Optional) If your team prefers storing connections as environment variables or in a supported secrets backend, the connection URI
        for this example would look like this:

            .. code-block:: text

                AIRFLOW_CONN_POSTGRES_DEFAULT=postgres://your_postgres_username:password@34.29.167.133:5432/your_postgres_db_name

        3. With the dependencies and connection in place, you can pass this connection to a DbtTaskGroup, DbtDag, or Dbt Operator
        Class via the `conn_id` parameter:

        .. note::
            Since the Airflow Connections for postgres do not provide a field for a default schema parameter, you will need to
            add it via the `dbt_args`:

        .. code-block:: python

            jaffle_shop = DbtTaskGroup(
                conn_id="postgres_default",
                dbt_args={
                    "schema": "my_schema",
                }
            )

Overriding connection Parameters
--------------------------------
Although setting up a database name in your Airflow Connection is required, The database/schema values from the
connection object can be overriden when instantiating a Cosmos Object:

.. code-block:: python

    jaffle_shop = DbtTaskGroup(
        conn_id="your_conn_id",
        dbt_args={
            "db_name": "your_db_name", # overrides the db specified in the Airflow connection
            "schema": "your_schema_name", # overrides the schema specified in the Airflow connection
        }
    )

Additionally, if you've specified a database/schema in either the connection object or the parameters shown in the code
block above, those are overriden by dbt project files. For example, if you've setup a `properties.yml` file in your dbt
project like this:

.. code-block:: yaml

    version: 2

    models:
      - name: customers
        description: table description
        config:
            schema: some_other_schema

Then the customers table will be written to `some_other_schema` instead of the `your_schema_name` provided in the
`DbtTaskGroup` class. These same rules apply with any `.sql` file in the `/models` directory or the `dbt_project.yml`
file.
