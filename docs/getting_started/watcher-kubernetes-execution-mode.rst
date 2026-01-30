.. _watcher-kubernetes-execution-mode:

``ExecutionMode.WATCHER_KUBERNETES``: High-Performance dbt Execution in Kubernetes
===================================================================================

.. versionadded:: 1.13.0

The ``ExecutionMode.WATCHER_KUBERNETES`` combines the **speed of the** :ref:`watcher-execution-mode` **with the isolation of** :ref:`kubernetes`.

This execution mode is ideal for users who:

* Want to leverage the performance benefits of the watcher execution mode
* Need to run dbt in isolated Kubernetes pods
* Prefer not to install dbt in their Airflow deployment

-------------------------------------------------------------------------------

Background
----------

The :ref:`watcher-execution-mode` introduced in Cosmos 1.11.0 significantly reduces dbt pipeline run times by running dbt as a single command while maintaining model-level observability in Airflow.

However, the original ``ExecutionMode.WATCHER`` requires dbt to be installed alongside Airflow. The ``ExecutionMode.WATCHER_KUBERNETES`` removes this limitation by running the dbt command inside Kubernetes pods, similar to ``ExecutionMode.KUBERNETES``.

For more details on the watcher concept and how it works, please refer to the :ref:`watcher-execution-mode` documentation.

-------------------------------------------------------------------------------

How to Use
----------

Users previously using ``ExecutionMode.KUBERNETES`` can simply replace the ``execution_mode`` to use ``ExecutionMode.WATCHER_KUBERNETES``.

The following example shows how to configure a ``DbtDag`` with ``ExecutionMode.WATCHER_KUBERNETES``:

.. code-block:: python

    from cosmos import DbtDag
    from cosmos.config import ExecutionConfig
    from cosmos.constants import ExecutionMode

    dag = DbtDag(
        dag_id="jaffle_shop_watcher_kubernetes",
        # ... other DAG parameters ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER_KUBERNETES,
            dbt_project_path=K8S_PROJECT_DIR,
        ),
        operator_args={
            "image": DBT_IMAGE,
            "get_logs": True,
            "log_events_on_failure": True,
        },
    )

**Key differences from** ``ExecutionMode.KUBERNETES``:

* The ``execution_mode`` is set to ``ExecutionMode.WATCHER_KUBERNETES`` instead of ``ExecutionMode.KUBERNETES``
* The producer task runs the entire ``dbt build`` command in a single Kubernetes pod
* Consumer tasks (sensors) watch for the completion of their corresponding dbt models

For the complete setup including Kubernetes secrets, Docker image configuration, and profile setup, refer to the :ref:`kubernetes` documentation.

-------------------------------------------------------------------------------

Performance Gains
-----------------

Early benchmarks using the ``jaffle_shop_watcher_kubernetes`` DAG show significant improvements:

+-----------------------------------------------+------------------+
| Execution Mode                                | Total Runtime    |
+===============================================+==================+
| ``ExecutionMode.KUBERNETES``                  | 00:00:32.155     |
+-----------------------------------------------+------------------+
| ``ExecutionMode.WATCHER_KUBERNETES``          | 00:00:11.783     |
+-----------------------------------------------+------------------+

This represents approximately a **63% reduction** in total DAG runtime.

The performance improvement comes from:

* Running dbt as a single command (reducing Kubernetes pod startup overhead)
* Leveraging dbt's native threading capabilities
* Eliminating repeated dbt initialization for each model

-------------------------------------------------------------------------------

Known Limitations
-----------------

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Kubernetes Provider Version Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ExecutionMode.WATCHER_KUBERNETES`` does not work with older versions of the ``apache-airflow-providers-cncf-kubernetes`` provider (<=10.7.0).

Please ensure you have a compatible version installed:

.. code-block:: bash

    pip install "apache-airflow-providers-cncf-kubernetes>10.7.0"

We successfully tested against the most recent release of the provider (`10.12.2 <https://pypi.org/project/apache-airflow-providers-cncf-kubernetes/10.12.2/>`_).

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Support for KPO deferrable mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The producer node created by the ``ExecutionMode.WATCHER_KUBERNETES`` producer task can be set to deferrable mode as long as:

- The correct version of Airflow Kubernetes is installed (``>=10.12.2``). This version fixed a bug (`PR <https://github.com/apache/airflow/pull/58684>`_) that prevented setting callbacks and parsing the logs when the Kubernetes Operator run using ``deferrable``. The experience should be further improved once `this other PR is merged <https://github.com/apache/airflow/pull/60778>_`.

.. code-block:: bash

    pip install "apache-airflow-providers-cncf-kubernetes>=10.12.2"

- The arguments ``deferrable=True`` and ``is_delete_operator_pod=True`` are set:

.. code-block: python

    dag = DbtDag(
        dag_id="jaffle_shop_watcher_kubernetes",
        # ... other DAG parameters ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER_KUBERNETES,
            dbt_project_path=K8S_PROJECT_DIR,
        ),
        operator_args={
            "deferrable": True,
            "is_delete_operator_pod": True,
            "image": DBT_IMAGE,
            "get_logs": True,
            "log_events_on_failure": True,
        },
    )

Conversely, the consumer tasks that subclass ``DbtConsumerWatcherKubernetesSensor`` run in deferrable mode by default when operating as a sensor.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Mandatory ``operator_args``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The  ``operator_args`` must define ``get_logs`` and ``log_events_on_failure``:

.. code-block: python

    dag = DbtDag(
        dag_id="jaffle_shop_watcher_kubernetes",
        # ... other DAG parameters ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER_KUBERNETES,
            dbt_project_path=K8S_PROJECT_DIR,
        ),
        operator_args={
            # ... other KPO mandatory args ...
            "get_logs": True,
            "log_events_on_failure": True,
        },
    )


~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Other Inherited Limitations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following limitations from ``ExecutionMode.WATCHER`` also apply to ``ExecutionMode.WATCHER_KUBERNETES``:

* **Individual dbt Operators**: Only ``DbtSeedWatcherKubernetesOperator``, ``DbtSnapshotWatcherKubernetesOperator``, and ``DbtRunWatcherKubernetesOperator`` are implemented. The ``DbtTestWatcherKubernetesOperator`` is currently a placeholder.

* **Test behavior**: The ``TestBehavior.AFTER_EACH`` is not supported. Tests are run as part of the ``dbt build`` command by the producer task.

* **Source freshness nodes**: The ``dbt build`` command does not run source freshness checks.

For more details on these limitations, refer to the :ref:`watcher-execution-mode` documentation.

-------------------------------------------------------------------------------

Example DAG
-----------

Below is a complete example of a DAG using ``ExecutionMode.WATCHER_KUBERNETES``:

.. literalinclude:: ../../dev/dags/jaffle_shop_watcher_kubernetes.py
    :language: python

-------------------------------------------------------------------------------

Prerequisites
-------------

Before using ``ExecutionMode.WATCHER_KUBERNETES``, ensure you have:

1. A Kubernetes cluster configured and accessible from your Airflow deployment
2. A Docker image containing your dbt project and profile
3. The ``apache-airflow-providers-cncf-kubernetes`` provider installed (version >10.7.0)

For detailed setup instructions, refer to the :ref:`kubernetes` documentation.

-------------------------------------------------------------------------------

Summary
-------

``ExecutionMode.WATCHER_KUBERNETES`` provides:

* ✅ **~63% faster** dbt DAG runs compared to ``ExecutionMode.KUBERNETES``
* ✅ **Isolation** between dbt and Airflow dependencies
* ✅ **Model-level visibility** in Airflow
* ✅ **Easy migration** from ``ExecutionMode.KUBERNETES``

This execution mode is ideal for teams who want the performance benefits of the watcher mode while maintaining the isolation provided by Kubernetes execution.
