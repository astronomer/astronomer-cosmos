:py:mod:`cosmos.providers.dbt.core.profiles.bigquery`
=====================================================

.. py:module:: cosmos.providers.dbt.core.profiles.bigquery


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.bigquery.create_profile_vars_google_cloud_platform



Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.bigquery.bigquery_profile


.. py:data:: bigquery_profile



.. py:function:: create_profile_vars_google_cloud_platform(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup
   https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html
