.. _lineage:

Configuring Lineage
===================

Since Cosmos 1.1, it uses internally the `openlineage-integration-common <https://github.com/OpenLineage/OpenLineage/tree/main/integration/common>`_
to parse artefacts generated with dbt commands and create lineage events.

To emit lineage events, Cosmos can use one of the following:

1. Airflow `official OpenLineage provider <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/1.0.2/guides/user.html>`_, or
2. `Additional libraries <https://openlineage.io/docs/integrations/airflow/>`_.

No change to the user DAG files is required to use OpenLineage.


Known limitations
-----------------

This feature is only available for the local
and virtualenv execution methods (read `execution modes <../getting_started/execution-modes.html>`_ for more information).

Additionally, since Cosmos uses the open-source `openlineage-integration-common <https://github.com/OpenLineage/OpenLineage/tree/main/integration/common>`_, it relies on this library to support specific dbt adapters. As of 27 December 2024, the version 1.26.0 of this package supports:

* Athena
* BigQuery
* Databricks
* DuckDB
* Dremio
* Postgres
* Redshift
* Snowflake
* Spark
* SQLServer

Contributions are also welcome in the `OpenLineage project <https://github.com/OpenLineage/OpenLineage/blob/main/integration/common/openlineage/common/provider/dbt/processor.py#L36C1-L47C22>`_ to support more adaptors.

Installation
------------

If using Airflow 2.7 or higher, install ``apache-airflow-providers-openlineage``.

Otherwise, install Cosmos using ``astronomer-cosmos[openlineage]``.


Configuration
-------------

If using Airflow 2.7, follow `the instructions <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/1.0.2/guides/user.html>`_ on how to configure OpenLineage.

Otherwise, follow `these instructions <https://openlineage.io/docs/integrations/airflow/>`_.


Namespace
.........

Cosmos will use the Airflow ``[openlineage]`` ``namespace`` property as a namespace, `if available <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/1.0.2/guides/user.html>`_.

Otherwise, it attempts to use the environment variable ``OPENLINEAGE_NAMESPACE`` as the namespace.

Finally, if neither are defined, it uses ``"cosmos"`` as the namespace.
