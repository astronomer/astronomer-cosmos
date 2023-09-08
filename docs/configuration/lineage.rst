.. _lineage:

Configuring Lineage
===================

Since Cosmos 1.1, it uses internally the `openlineage-integration-common <https://github.com/OpenLineage/OpenLineage/tree/main/integration/common>`_
to parse artifacts generated with dbt commands and create lineage events. This feature is only available for the local
and virtualenv execution methods (read `execution modes <../getting_started/execution-modes.html>`_ for more information).

To emit lineage events, Cosmos can use one of the following:

1. Airflow 2.7 `built-in support to OpenLineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/1.0.2/guides/user.html>`_, or
2. `Additional libraries <https://openlineage.io/docs/integrations/airflow/>`_.

No change to the user DAG files is required to use OpenLineage.


Installation
------------

If using Airflow 2.7, no other dependency is required.

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
