Airflow 3 Compatibility (First Iteration)
=========================================

The Cosmos 1.10.0 release marks the **first iteration** of adding compatibility for `Apache Airflow® 3 <https://airflow.apache.org/>`_
This is an important milestone as we work towards ensuring that Cosmos seamlessly integrates with the latest advancements in the Airflow ecosystem.

What's Been Tested
------------------

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

Experimental Features
---------------------

We are actively validating the combined support for `Assets <https://airflow.apache.org/docs/apache-airflow/3.0.0/authoring-and-scheduling/assets.html>`_
and `OpenLineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html>`_ with Airflow 3.
While early results are promising, we are marking this integration as **experimental** in Cosmos 1.10.0.
We encourage users to try it out and provide feedback, but note that certain edge cases may still be under
investigation.

Known Observations
------------------

We have observed that the `cosmos_callback_dag.py <https://github.com/astronomer/astronomer-cosmos/blob/main/dev/dags/cosmos_callback_dag.py>`_
DAG is experiencing noticeable **performance lag** in Airflow 3. This DAG leverages the ``ObjectStoragePath`` module
along with several libraries from the **fsspec** ecosystem.
We are prioritizing a deeper investigation into this issue and will provide updates as optimizations or fixes become
available in future releases.

----

We are excited to bring Airflow 3 compatibility to Cosmos and appreciate the community's feedback as we refine these
capabilities.

Stay tuned for continued improvements and enhancements.
