Airflow 3 Compatibility (First Iteration)
=========================================

The Cosmos 1.10.0 release marks the **first iteration** of adding compatibility for `Apache Airflow® 3 <https://airflow.apache.org/>`_
This is an important milestone as we work towards ensuring that Cosmos seamlessly integrates with the latest advancements in the Airflow ecosystem.

Breaking changes
----------------

Airflow Asset (Dataset) URIs validation rules changed in Airflow 3.0.0 and OpenLineage URIs (standard used by Cosmos) are no longer valid in Airflow 3.

Therefore, if using Cosmos with Airflow 3, the Airflow Dataset URIs will be changed to use backslashes instead of dots to separate the schema and table name.

Example of Airflow 2 Cosmos Dataset URI:

- postgres://0.0.0.0:5434/postgres.public.orders

Example of Airflow 3 Cosmos Asset URI:

- postgres://0.0.0.0:5434/postgres/public/orders


If you want to use the Airflow 3 URI standard while still using Airflow 2, please set:

.. code-block:: bash

    export AIRFLOW__COSMOS__USE_DATASET_AIRFLOW3_URI_STANDARD=1

.. warning::
    Remember to update any DAGs that are triggered using Cosmos-generated datasets or aliases to the new URI format.


What Works
----------

With the changes contributed in Cosmos 1.10.0 to bring in compatibility with Airflow 3, we have validated Cosmos’
functionality with Airflow 3 by extending our CI infrastructure:

- **All example DAGs** in the `dev/dags <https://github.com/astronomer/astronomer-cosmos/tree/main/dev/dags>`_ directory of the repository are tested against Airflow 3.
- Our `CI test matrix <https://github.com/astronomer/astronomer-cosmos/blob/main/.github/workflows/test.yml>`_ now includes explicit entries to run the following tests on Airflow 3:
  - Unit Tests
  - Integration Tests
  - Performance Tests
  - Kubernetes Tests

These additions ensure that all core functionality, workflows, and integrations provided by Cosmos continue to operate
reliably under Airflow 3.

Multiple dbt docs in Airflow 3 UI
---------------------------------

There have been significant changes to how plugins work in Airflow 3.x. Cosmos now supports Airflow 3 FastAPI plugins for UI integration and hosting dbt docs via external views.
Cosmos registers a FastAPI sub-application at ``/cosmos`` and adds menu entries under **Browse**. Configure one or more projects in ``airflow.cfg`` under the ``[cosmos]`` section:

.. code-block:: ini

   [cosmos]
   # Map of slug -> {dir, conn_id, index, name}
   dbt_docs_projects = {
     "core": {"dir": "/path/to/core/target", "index": "index.html", "name": "dbt Docs (Core)"},
     "mart": {"dir": "s3://bucket/path/to/mart/target", "conn_id": "aws_default", "name": "dbt Docs (Mart)"}
   }

You can set the same mapping via the Airflow config environment variable ``AIRFLOW__COSMOS__DBT_DOCS_PROJECTS``.

.. code-block:: bash

   # Shell (note the single quotes to preserve JSON)
   export AIRFLOW__COSMOS__DBT_DOCS_PROJECTS='{"core":{"dir":"/path/to/core/target","index":"index.html","name":"dbt Docs (Core)"},"mart":{"dir":"s3://bucket/path/to/mart/target","conn_id":"aws_default","name":"dbt Docs (Mart)"}}'

.. code-block:: dockerfile

   # Dockerfile (ENV does not require outer quotes)
   ENV AIRFLOW__COSMOS__DBT_DOCS_PROJECTS={"core":{"dir":"/path/to/core/target","index":"index.html","name":"dbt Docs (Core)"},"mart":{"dir":"s3://bucket/path/to/mart/target","conn_id":"aws_default","name":"dbt Docs (Mart)"}}

.. code-block:: yaml

   # Docker Compose / Kubernetes
   environment:
     AIRFLOW__COSMOS__DBT_DOCS_PROJECTS: '{"core":{"dir":"/path/to/core/target","index":"index.html","name":"dbt Docs (Core)"},"mart":{"dir":"s3://bucket/path/to/mart/target","conn_id":"aws_default","name":"dbt Docs (Mart)"}}'

Docs are available at ``/cosmos/<slug>/dbt_docs_index.html``. Static assets are served directly for local directories or proxied for remote storage (S3/GCS/Azure/HTTP).

Validation in Progress
----------------------

We are actively validating the combined support for `Assets <https://airflow.apache.org/docs/apache-airflow/3.0.0/authoring-and-scheduling/assets.html>`_
and `OpenLineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html>`_ with Airflow 3.
This may be **unstable** in Cosmos 1.10.0. Bug reports are very welcome.
We encourage users to try it out and provide feedback, but note that certain edge cases may still be under
investigation.

Known Limitations
-----------------

Airflow 3 DatasetAlias no longer support ASCII characters. This issue has been reported to the `Airflow community <https://github.com/apache/airflow/issues/51566>`_
and we are also tracking it in the `Cosmos repository <https://github.com/astronomer/astronomer-cosmos/issues/1802>`_.

What's Next
-----------

We are actively tracking open issues and enhancements related to **Airflow 3 compatibility** in Cosmos.
You can view the full list of currently open issues on GitHub here:

- `Open Airflow 3 Support Issues on GitHub <https://github.com/astronomer/astronomer-cosmos/issues?q=is%3Aissue%20state%3Aopen%20label%3Asupport%3Aairflow3>`_

We encourage community members to follow, comment, or contribute to any relevant discussions.

We are excited to bring Airflow 3 compatibility to Cosmos and appreciate the community's feedback as we refine these capabilities.

Stay tuned for continued improvements and enhancements.
