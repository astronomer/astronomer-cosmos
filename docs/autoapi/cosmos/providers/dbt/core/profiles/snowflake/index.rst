:py:mod:`cosmos.providers.dbt.core.profiles.snowflake`
======================================================

.. py:module:: cosmos.providers.dbt.core.profiles.snowflake


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.snowflake.get_snowflake_account
   cosmos.providers.dbt.core.profiles.snowflake.create_profile_vars_snowflake



Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.snowflake.snowflake_profile


.. py:data:: snowflake_profile



.. py:function:: get_snowflake_account(account: str, region: str | None = None) -> str


.. py:function:: create_profile_vars_snowflake(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup
   https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
