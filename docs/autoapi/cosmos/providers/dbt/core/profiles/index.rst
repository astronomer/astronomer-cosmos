:py:mod:`cosmos.providers.dbt.core.profiles`
============================================

.. py:module:: cosmos.providers.dbt.core.profiles


Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   bigquery/index.rst
   databricks/index.rst
   postgres/index.rst
   redshift/index.rst
   snowflake/index.rst


Package Contents
----------------

Classes
~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.AdapterConfig



Functions
~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.create_profile_vars_google_cloud_platform
   cosmos.providers.dbt.core.profiles.create_profile_vars_databricks
   cosmos.providers.dbt.core.profiles.create_profile_vars_postgres
   cosmos.providers.dbt.core.profiles.create_profile_vars_redshift
   cosmos.providers.dbt.core.profiles.create_profile_vars_snowflake
   cosmos.providers.dbt.core.profiles.get_available_adapters



Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.bigquery_profile
   cosmos.providers.dbt.core.profiles.databricks_profile
   cosmos.providers.dbt.core.profiles.postgres_profile
   cosmos.providers.dbt.core.profiles.redshift_profile
   cosmos.providers.dbt.core.profiles.snowflake_profile


.. py:data:: bigquery_profile



.. py:function:: create_profile_vars_google_cloud_platform(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup
   https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html


.. py:function:: create_profile_vars_databricks(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/databricks-setup
   https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html

   Database override is used to reference a Unity Catalog which was made available in dbt-databricks>=1.1.1
   Airflow recommends specifying token in the password field as it's more secure.
   If the host contains the https then we remove it.


.. py:data:: databricks_profile



.. py:function:: create_profile_vars_postgres(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/postgres-setup
   https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html


.. py:data:: postgres_profile



.. py:function:: create_profile_vars_redshift(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
   https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html


.. py:data:: redshift_profile



.. py:function:: create_profile_vars_snowflake(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup
   https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html


.. py:data:: snowflake_profile



.. py:class:: AdapterConfig

   .. py:attribute:: profile_name
      :type: str



   .. py:attribute:: profile
      :type: Dict[str, str]



   .. py:attribute:: create_profile_function
      :type: Callable[[airflow.models.Connection, Optional[str], Optional[str]], Tuple[str, Dict[str, str]]]




.. py:function:: get_available_adapters() -> Dict[str, AdapterConfig]
