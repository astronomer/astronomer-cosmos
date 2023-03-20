:py:mod:`cosmos.providers.dbt.core.profiles.postgres`
=====================================================

.. py:module:: cosmos.providers.dbt.core.profiles.postgres


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.postgres.create_profile_vars_postgres



Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.postgres.postgres_profile


.. py:data:: postgres_profile



.. py:function:: create_profile_vars_postgres(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/postgres-setup
   https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html
