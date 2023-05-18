Connections and Profiles
==========================

Cosmos automatically translates Airflow connections to dbt profiles. This means that you can use the same connection
objects you use in Airflow to authenticate with your database in dbt. To do so, there's a class in Cosmos for each
Airflow connection to dbt profile mapping.

Each profile mapping class typically gets defined with the following attributes:

* ``airflow_connection_type``: the Airflow connection type that this profile mapping is for.
* ``required_fields``: a list of required fields for the profile. This refers to the field as it is in dbt.
* ``secret_fields``: a list of fields that are secret. These fields will be passed to dbt as environment variables.
* ``airflow_param_mapping``: a dictionary that maps the Airflow connection fields to the dbt profile fields. The keys
  are the Airflow connection fields and the values are the dbt profile fields.
* Optionally, a profile mapping can specify a ``transform_{dbt_field_name}`` function for each dbt profile field. This
  function will be called on the value of the Airflow connection field before it is passed to dbt. This is useful for
  transforming the value of a field before it is passed to dbt. For example, sometimes ``host`` fields need to be passed
  to dbt without the ``http://`` prefix.

Because of this, the profile mapping classes are self-documenting. You can see the available profile mappings below.

Specifying Values
-----------------

The dbt profile values generally come from one of two places:

1. The ``profile_args`` parameter that you pass into either ``DbtDag`` or ``DbtTaskGroup``.
2. The Airflow connection values.

Any value can be overridden by the ``profile_args`` parameter, because that value always takes precedence over the
Airflow connection value. For example, if you pass in a ``user`` value in ``profile_args``, that value will be used
instead of the Airflow connection value, even if you have a value for ``user`` in the Airflow connection.

You can also specify values in the ``profile_args`` to be put in the dbt profile. This is useful for specifying values
that are not in the Airflow connection.

Secret Fields
-------------

Secret fields are passed to dbt as environment variables. This is to avoid writing the secret values to disk. The
secret values are passed to dbt as environment variables with the following naming convention:

``COSMOS_CONN_{AIRFLOW_CONNECTION_TYPE}_{FIELD_NAME}``

For example, a Snowflake password field would be passed to dbt as an environment variable with the name
``COSMOS_CONN_SNOWFLAKE_PASSWORD``.


Available Profile Mappings
==========================


Google Cloud Platform
---------------------

Service Account File
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.bigquery.GoogleCloudServiceAccountFileProfileMapping
    :undoc-members:
    :members:


Databricks
----------

Token
~~~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.databricks.DatabricksTokenProfileMapping
    :undoc-members:
    :members:


Exasol
------

Username and Password
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.exasol.ExasolUserPasswordProfileMapping
    :undoc-members:
    :members:


Postgres
--------

Username and Password
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.postgres.PostgresUserPasswordProfileMapping
    :undoc-members:
    :members:


Redshift
--------

Username and Password
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.redshift.RedshiftUserPasswordProfileMapping
    :undoc-members:
    :members:


Snowflake
---------

Username and Password
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.snowflake.SnowflakeUserPasswordProfileMapping
    :undoc-members:
    :members:


Spark
-----

Thrift
~~~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.spark.SparkThriftProfileMapping
    :undoc-members:
    :members:


Trino
-----

Base
~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.trino.TrinoBaseProfileMapping
    :undoc-members:
    :members:


LDAP
~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.trino.TrinoLDAPProfileMapping
    :undoc-members:
    :members:
    :show-inheritance:


JWT
~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.trino.TrinoJWTProfileMapping
    :undoc-members:
    :members:
    :show-inheritance:

Certificate
~~~~~~~~~~~

.. autoclass:: cosmos.providers.dbt.core.profiles.trino.TrinoCertificateProfileMapping
    :undoc-members:
    :members:
    :show-inheritance:
