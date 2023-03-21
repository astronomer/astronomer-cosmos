:py:mod:`cosmos.providers.dbt.core.profiles.databricks`
=======================================================

.. py:module:: cosmos.providers.dbt.core.profiles.databricks


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.databricks.create_profile_vars_databricks



Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.databricks.logger
   cosmos.providers.dbt.core.profiles.databricks.databricks_profile


.. py:data:: logger



.. py:data:: databricks_profile



.. py:function:: create_profile_vars_databricks(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/databricks-setup
   https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html

   Database override is used to reference a Unity Catalog which was made available in dbt-databricks>=1.1.1
   Airflow recommends specifying token in the password field as it's more secure.
   If the host contains the https then we remove it.
