.. _gcp-gke:


GCP GKE execution modes (experimental)
======================================

.. versionadded:: 1.15.0

.. warning::

   This feature is **experimental** and may change without a deprecation period.

Cosmos provides two execution modes to run dbt in `Google Managed Airflow Gen 3 <https://cloud.google.com/products/managed-service-for-apache-airflow>`_ (formerly Cloud Composer 3) with a self-managed custom GKE cluster:

- ``ExecutionMode.GCP_GKE``: the GKE variant of :ref:`kubernetes`.
- ``ExecutionMode.WATCHER_GCP_GKE``: the GKE variant of :ref:`watcher-kubernetes-execution-mode`.

Both modes work the same way as their Kubernetes counterparts but use the
`GKEStartPodOperator <https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/kubernetes_engine/index.html#airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator>`_
instead of ``KubernetesPodOperator``, which handles GKE authentication automatically and lets you select the cluster to use.

To use either mode, install the Google Cloud provider:

.. code-block:: bash

    pip install "astronomer-cosmos[google]"

Both modes require three additional parameters in ``operator_args``:

- ``project_id``: Your GCP project ID
- ``location``: The GKE cluster location (e.g. ``us-central1``)
- ``cluster_name``: The GKE cluster name


.. _gcp-gke-execution-mode:

GCP GKE execution mode
~~~~~~~~~~~~~~~~~~~~~~~

``ExecutionMode.GCP_GKE`` is the GCP GKE variant of ``ExecutionMode.KUBERNETES``.

.. code-block:: python

    from cosmos import DbtDag
    from cosmos.config import ExecutionConfig
    from cosmos.constants import ExecutionMode

    dag = DbtDag(
        dag_id="jaffle_shop_gcp_gke",
        # ... other DAG parameters ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.GCP_GKE,
            dbt_project_path="dags/dbt/jaffle_shop",
        ),
        operator_args={
            "image": "dbt-jaffle-shop:1.0.0",
            "get_logs": True,
            "project_id": "my-gcp-project",
            "location": "us-central1",
            "cluster_name": "my-gke-cluster",
        },
    )

All other configuration (Docker image, secrets, profiles) and the :ref:`limitations <kubernetes-known-limitations>`
are the same as ``ExecutionMode.KUBERNETES``. For detailed setup instructions, refer to the
:ref:`kubernetes` documentation.


.. _watcher-gcp-gke-execution-mode:

Watcher GCP GKE execution mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ExecutionMode.WATCHER_GCP_GKE`` is the GCP GKE variant of ``ExecutionMode.WATCHER_KUBERNETES``.

.. code-block:: python

    from cosmos import DbtDag
    from cosmos.config import ExecutionConfig
    from cosmos.constants import ExecutionMode

    dag = DbtDag(
        dag_id="jaffle_shop_watcher_gcp_gke",
        # ... other DAG parameters ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER_GCP_GKE,
            dbt_project_path="dags/dbt/jaffle_shop",
        ),
        operator_args={
            "image": "dbt-jaffle-shop:1.0.0",
            "get_logs": True,
            "project_id": "my-gcp-project",
            "location": "us-central1",
            "cluster_name": "my-gke-cluster",
        },
    )

All limitations and configuration from ``ExecutionMode.WATCHER_KUBERNETES`` apply. For more details, refer to
the :ref:`watcher-kubernetes-execution-mode` documentation.
