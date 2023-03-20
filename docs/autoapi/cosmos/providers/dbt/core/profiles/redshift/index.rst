:py:mod:`cosmos.providers.dbt.core.profiles.redshift`
=====================================================

.. py:module:: cosmos.providers.dbt.core.profiles.redshift


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.redshift.create_profile_vars_redshift



Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.core.profiles.redshift.redshift_profile


.. py:data:: redshift_profile



.. py:function:: create_profile_vars_redshift(conn: airflow.models.Connection, database_override: str | None = None, schema_override: str | None = None) -> tuple[str, dict[str, str]]

   https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
   https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html
