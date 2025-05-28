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

`
export AIRFLOW__COSMOS__USE_DATASET_AIRFLOW3_URI_STANDARD=1
`

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

Validation in Progress
----------------------

We are actively validating the combined support for `Assets <https://airflow.apache.org/docs/apache-airflow/3.0.0/authoring-and-scheduling/assets.html>`_
and `OpenLineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html>`_ with Airflow 3.
This may be **unstable** in Cosmos 1.10.0. Bug reports are very welcome.
We encourage users to try it out and provide feedback, but note that certain edge cases may still be under
investigation.

Known Limitations
-----------------

We have observed that the `cosmos_callback_dag.py <https://github.com/astronomer/astronomer-cosmos/blob/main/dev/dags/cosmos_callback_dag.py>`_
DAG is experiencing noticeable **performance lag** in Airflow 3. This DAG leverages the ``ObjectStoragePath`` module
along with several libraries from the **fsspec** ecosystem.
We are prioritizing a deeper investigation into this issue and will provide updates as optimizations or fixes become
available in future releases.

There have been significant changes to how plugins work in Airflow 3.0, and more changes are coming in Airflow 3.1.
Even though the Cosmos dbt docs plugin is not currently working, we are actively working on supporting this feature.

What's Next
-----------

We are actively tracking open issues and enhancements related to **Airflow 3 compatibility** in Cosmos.
You can view the full list of currently open issues on GitHub here:

- `Open Airflow 3 Support Issues on GitHub <https://github.com/astronomer/astronomer-cosmos/issues?q=is%3Aissue%20state%3Aopen%20label%3Asupport%3Aairflow3>`_

We encourage community members to follow, comment, or contribute to any relevant discussions.

We are excited to bring Airflow 3 compatibility to Cosmos and appreciate the community's feedback as we refine these capabilities.

Stay tuned for continued improvements and enhancements.
