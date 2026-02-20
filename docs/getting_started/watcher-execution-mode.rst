.. _watcher-execution-mode:

Introducing ``ExecutionMode.WATCHER``: Experimental High-Performance dbt Execution in Cosmos
============================================================================================

With the release of **Cosmos 1.11.0**, we are introducing a powerful new experimental execution mode — ``ExecutionMode.WATCHER`` — designed to drastically reduce dbt pipeline run times in Airflow.

Early benchmarks show that ``ExecutionMode.WATCHER`` can cut total DAG runtime **by up to 80%**, bringing performance **on par with running dbt CLI locally**. Since this execution mode improves the performance by leveraging `dbt threading <https://docs.getdbt.com/docs/running-a-dbt-project/using-threads>`_ and Airflow deferrable sensors, the performance gains will depend on three major factors:

- The amount of dbt ``threads`` set either via the dbt profile configuration or the dbt ``--threads`` flag
- The topology of the dbt pipeline
- The ``poke_interval`` and ``timeout`` settings of the ``DbtConsumerWatcherSensor`` operator, which determine the frequency and duration of the sensor's polling.

-------------------------------------------------------------------------------

Background: The Problem with the Local Execution Mode in Cosmos
---------------------------------------------------------------

When running dbt via Cosmos using the default ``ExecutionMode.LOCAL``, each dbt model is executed as a separate Airflow task.

This provides strong observability and task-level retry control — but it comes at a cost. Each model runs a new dbt process, which introduces significant overhead.

Consider the `google/fhir-dbt-analytics <https://github.com/google/fhir-dbt-analytics>`_ project:

+-------------------------------------------------------------+-----------------------------------+------------------+
| Run Type                                                    | Description                       | Total Runtime    |
+=============================================================+===================================+==================+
| Single ``dbt run`` (dbt CLI)                                | Runs the whole DAG in one command | ~5m 30s          |
+-------------------------------------------------------------+-----------------------------------+------------------+
| One ``dbt run`` per model, totalling 184 commands (dbt CLI) | Each model is its own task        | ~32m             |
+-------------------------------------------------------------+-----------------------------------+------------------+

This difference motivated a rethinking of how Cosmos interacts with dbt.

-------------------------------------------------------------------------------

Concept: ``ExecutionMode.WATCHER``
----------------------------------

``ExecutionMode.WATCHER`` combines the **speed of a single dbt run** with the **observability and task management of Airflow**.

It is built on two operator types:

* ``DbtProducerWatcherOperator`` (`#1982 <https://github.com/astronomer/astronomer-cosmos/pull/1982>`_)
  Runs dbt **once** across the entire pipeline, register to `dbt event callbacks <https://docs.getdbt.com/reference/programmatic-invocations#registering-callbacks>`_ and sends model progress updates via Airflow **XComs**.

* ``DbtConsumerWatcherSensor`` (`#1998 <https://github.com/astronomer/astronomer-cosmos/pull/1998>`_)
  Watches those XComs and marks individual Airflow tasks as complete when their corresponding dbt models finish.

Together, these operators let you:

* Run dbt as a single command (for speed)
* Retain model-level observability (for clarity)
* Retry specific models (for resilience)

-------------------------------------------------------------------------------

Performance Gains
-----------------

We used a dbt project developed by Google, the `google/fhir-dbt-analytics <https://github.com/google/fhir-dbt-analytics>`_ project, that interfaces with BigQuery. It contains:
* 2 seeds
* 52 sources
* 185 models

Initial benchmarks, using  illustrate significant improvements:

+-----------------------------------------------+-----------+--------------------+
| Environment                                   | Threads   | Execution Time (s) |
+===============================================+===========+====================+
| dbt build (dbt CLI)                           | 4         | 6–7                |
+-----------------------------------------------+-----------+--------------------+
| dbt run per model (dbt CLI)                   | —         | 30                 |
| similar to the Cosmos ``ExecutionMode.LOCAL`` |           |                    |
+-----------------------------------------------+-----------+--------------------+
| Cosmos ``ExecutionMode.LOCAL`` (Astro CLI)    | —         | 10–15              |
+-----------------------------------------------+-----------+--------------------+
| Cosmos ``ExecutionMode.WATCHER`` (Astro CLI)  | 1         | 26                 |
|                                               | 2         | 14                 |
|                                               | 4         | 7                  |
|                                               | 8         | 4                  |
|                                               | 16        | 2                  |
+-----------------------------------------------+-----------+--------------------+
| Cosmos ``ExecutionMode.WATCHER`` (Astro Cloud | 8         | ≈5                 |
| Standard Deployment with A10 workers          |           |                    |
+-----------------------------------------------+-----------+--------------------+

The last line represents the performance improvement in a real-world Airflow deployment, using `Astro Cloud <https://www.astronomer.io/>`_.

Depending on the dbt workflow topology, if your dbt DAG previously took 5 minutes with ``ExecutionMode.LOCAL``, you can expect it to complete in roughly **1 minute** with ``ExecutionMode.WATCHER``.

We plan to repeat these benchmarks and share the code with the community in the future.


.. note::
   ``ExecutionMode.WATCHER`` relies on the ``threads`` value defined in your dbt profile. Start with a conservative value that matches the CPU capacity of your Airflow workers, then gradually increase it to find the sweet spot between faster runs and acceptable memory/CPU usage.

When we ran the `astronomer/cosmos-benchmark <https://github.com/astronomer/cosmos-benchmark>`_ project with ``ExecutionMode.WATCHER``, that same ``threads`` setting directly affected runtime: moving from 1 to 8 threads reduced the end-to-end ``dbt build`` duration from roughly 26 seconds to about 4 seconds (see table above), while 16 threads squeezed it to around 2 seconds at the cost of higher CPU usage. Use those numbers as a reference point when evaluating how thread counts scale in your own environment.

To increase the number of threads, edit your dbt ``profiles.yml`` (or Helm values if you manage the profile there) and update the ``threads`` key for the target you use with Cosmos:

.. code-block:: yaml

   your_dbt_project:
     target: prod
     outputs:
       prod:
         type: postgres
         host: your-host
         user: your-user
         password: your-password
         schema: analytics
         threads: 8  # increase or decrease to match available resources


If you prefer to manage threads through Cosmos profile mappings instead of editing ``profiles.yml`` directly, pass ``profile_args={"threads": <int>}`` to your ``ProfileConfig``. For example, using the built-in ``PostgresUserPasswordProfileMapping``:

.. code-block:: python

   from cosmos.config import ProfileConfig
   from cosmos.profiles import PostgresUserPasswordProfileMapping

   profile_config = ProfileConfig(
       profile_name="jaffle_shop",
       target_name="prod",
       profile_mapping=PostgresUserPasswordProfileMapping(
           conn_id="postgres_connection",
           profile_args={"threads": 8},
       ),
   )


-------------------------------------------------------------------------------

Example Usage of ``ExecutionMode.WATCHER``
------------------------------------------

There are two main ways to use the new execution mode in Cosmos — directly within a ``DbtDag``, or embedded as part of a ``DbtTaskGroup`` inside a larger DAG.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Example 1 — Using ``DbtDag`` with ``ExecutionMode.WATCHER``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can enable WATCHER mode directly in your ``DbtDag`` configuration.
This approach is best when your Airflow DAG is fully dedicated to a dbt project.

.. literalinclude:: ../../dev/dags/example_watcher.py
    :language: python
    :start-after: [START example_watcher]
    :end-before: [END example_watcher]

As it can be observed, the only difference with the default ``ExecutionMode.LOCAL`` is the addition of the ``execution_config`` parameter with the ``execution_mode`` set to ``ExecutionMode.WATCHER``. The ``ExecutionMode`` enum can be imported from ``cosmos.constants``. For more information on the ``ExecutionMode.LOCAL``, please, check the `dedicated page <execution-modes.html#local>`__

**How it works:**

* Cosmos executes your dbt project once via a producer task.
* Model-level Airflow tasks act as watchers or sensors, updating their state as dbt completes each model.
* The DAG remains fully observable and retryable, with **dramatically improved runtime performance** (often 5× faster than ``ExecutionMode.LOCAL``).

**How it looks like:**

.. image:: /_static/jaffle_shop_watcher_dbt_dag_dag_run.png
    :alt: Cosmos DbtDag with `ExecutionMode.WATCHER`
    :align: center

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Example 2 — Using ``DbtTaskGroup`` with ``ExecutionMode.WATCHER``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your Airflow DAG includes multiple stages or integrations (e.g., data ingestion → dbt → reporting), use ``DbtTaskGroup`` to embed your dbt project into a larger DAG — still benefiting from WATCHER performance.

.. code-block:: python
   :caption: example_watcher_taskgroup.py
   :name: example_watcher_taskgroup

    from airflow.models import DAG
    from airflow.operators.empty import EmptyOperator
    from cosmos import DbtTaskGroup

    with DAG(
        dag_id="example_watcher_taskgroup",
        schedule="@daily",
        start_date=datetime(2023, 1, 1),
        catchup=False,
    ):
        """
        The simplest example of using Cosmos to render a dbt project as a TaskGroup.
        """
        pre_dbt = EmptyOperator(task_id="pre_dbt")

        first_dbt_task_group = DbtTaskGroup(
            group_id="first_dbt_task_group",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
            ),
            project_config=ProjectConfig(DBT_PROJECT_PATH),
            profile_config=profile_config,
            operator_args=operator_args,
        )

        pre_dbt >> first_dbt_task_group

**Key advantages:**

* Integrates seamlessly into complex Airflow DAGs.
* Uses the same high-performance producer/consumer execution model.
* Each ``DbtTaskGroup`` behaves independently — allowing modular dbt runs within larger workflows.

.. image:: /_static/jaffle_shop_watcher_dbt_taskgroup_dag_run.png
    :alt: Cosmos DbtDag with `ExecutionMode.WATCHER`
    :align: center

-------------------------------------------------------------------------------

Additional details
-------------------

~~~~~~~~~~~~~~~~
How retries work
~~~~~~~~~~~~~~~~

When the ``dbt build`` command run by ``DbtProducerWatcherOperator`` fails, it will notify all the ``DbtConsumerWatcherSensor``.

The individual watcher tasks that subclass ``DbtConsumerWatcherSensor`` can retry the dbt command themselves, using the same behavior as ``ExecutionMode.LOCAL``.

If a branch of the DAG fails, users can clear the status of a failed consumer task, including its downstream tasks, via the Airflow UI, and each of them will run in ``ExecutionMode.LOCAL``.

**Producer retry behavior**

.. versionadded:: 1.12.2

When the ``DbtProducerWatcherOperator`` is triggered for a retry (try_number > 1), it will not re-run the dbt build command and will succeed. In previous versions of Cosmos, the producer task would fail during retries.
This behavior is designed to support TaskGroup-level retries, as reported in `#2282 <https://github.com/astronomer/astronomer-cosmos/issues/2282>`_.

**Why this matters:**

- In earlier versions, attempting to retry the producer task would raise an ``AirflowException``, causing the retry to fail immediately.
- Now, the producer gracefully skips execution on retries, logging an informational message explaining that the retry was skipped to avoid running a second ``dbt build``.
- This allows users to retry entire TaskGroups and/or DAGs without the producer task blocking the retry flow.

**Important considerations:**

- The producer task should still be configured with ``retries=0`` (which Cosmos enforces by default) to avoid unintended duplicate ``dbt build`` runs.

- By default, Cosmos sets ``retries`` to ``0`` in``DbtProducerWatcherOperator``. Users can retry manually by clearing the status of the producer task and all its downstream tasks, keeping in mind that the producer task will not re-run the ``dbt build`` command and will succeed.

The overall retry behavior will be further improved once `#1978 <https://github.com/astronomer/astronomer-cosmos/issues/1978>`_ is implemented.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Watcher dbt Execution Queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 1.14.0

In watcher execution mode, by default, consumer sensor tasks are lightweight sensors that wait for the producer task to complete. On their first attempt, they require minimal CPU and memory resources. However, when these tasks retry, they execute the dbt command for the node, which may require significantly more resources.

The ``watcher_dbt_execution_queue`` configuration allows you to specify a different worker queue for retry attempts. This enables you to:

- **Optimize resource allocation** — Use lightweight workers for initial sensor execution and high-resource workers for retries
- **Improve scheduling efficiency** — Prevent resource contention between initial sensor tasks and retry executions
- **Scale independently** — Scale retry queues separately based on retry workload patterns

**Configuration:**

Set the ``watcher_dbt_execution_queue`` in your Airflow configuration:

.. code-block:: ini

   [cosmos]
   watcher_dbt_execution_queue = high_memory_queue

Or via environment variable:

.. code-block:: bash

   export AIRFLOW__COSMOS__WATCHER_DBT_EXECUTION_QUEUE=high_memory_queue

**How it works:**

- For watcher producer tasks (``DbtProducerWatcherOperator``), the configured queue is used during their first execution
- For watcher consumer tasks (``DbtConsumerWatcherSensor``), from their first retry onwards, if ``watcher_dbt_execution_queue`` is configured, the task is automatically assigned to the specified queue
- This behavior is enforced by Cosmos via an `Airflow cluster policy <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/cluster-policies.html>`_ (``task_instance_mutation_hook``) that mutates ``task_instance.queue`` at runtime for retry attempts

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Installation of Airflow and dbt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since Cosmos 1.12.0, ``ExecutionMode.WATCHER`` works well regardless of whether dbt and Airflow are installed in the same Python virtual environment.

When dbt and Airflow are installed in the same Python virtual environment, the ``ExecutionMode.WATCHER`` uses dbt `callback features <https://docs.getdbt.com/reference/programmatic-invocations#registering-callbacks>`_.

When dbt and Airflow are not installed in the same Python virtual environment, the ``ExecutionMode.WATCHER`` consumes the dbt `structured logging <https://docs.getdbt.com/reference/events-logging#structured-logging>`_ to update the consumer tasks.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Synchronous versus Asynchronous sensor execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In Cosmos 1.11.0, the ``DbtConsumerWatcherSensor`` operator is implemented as a synchronous XCom sensor, which continuously occupies the worker slot - even if they're just sleeping and checking periodically.

Starting with Cosmos 1.12.0, the ``DbtConsumerWatcherSensor`` supports
`deferrable (asynchronous) execution <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>`_. Deferrable execution frees up the Airflow worker slot, while task status monitoring is handled by the Airflow triggerer component,
which increases overall task throughput. By default, the sensor now runs in deferrable mode.

-------------------------------------------------------------------------------

Known Limitations
-------------------

~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Producer task implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The producer task is implemented as a ``DbtProducerWatcherOperator`` and currently relies on dbt being installed alongside the Airflow deployment, as in the ``ExecutionMode.LOCAL`` implementation.

The alternative to this implementation is to use ``ExecutionMode.WATCHER_KUBERNETES``, which is built on top of ``ExecutionMode.KUBERNETES``. Check :ref:`watcher-kubernetes-execution-mode` for more information.

~~~~~~~~~~~~~~~~~~~~~~~~
Individual dbt Operators
~~~~~~~~~~~~~~~~~~~~~~~~

The ``ExecutionMode.WATCHER`` efficiently implements the following operators:
* ``DbtSeedWatcherOperator``
* ``DbtSnapshotWatcherOperator``
* ``DbtRunWatcherOperator``

However, other operators that are available in the ``ExecutionMode.LOCAL`` mode are not implemented.

The ``DbtBuildWatcherOperator`` is not implemented, since the build command is executed by the producer ``DbtProducerWatcherOperator`` operator.

Additionally, since the ``dbt build`` command does not run ``source`` nodes, the operator ``DbtSourceWatcherOperator`` is equivalent to the ``DbtSourceLocalOperator`` operator, from ``ExecutionMode.LOCAL``.

Finally, the following features are not implemented as operators under ``ExecutionMode.WATCHER``:

* ``dbt ls``
* ``dbt run-operation``
* ``dbt docs``
* ``dbt clone``

You can still invoke these operators using the default ``ExecutionMode.LOCAL`` mode.

~~~~~~~~~~~~~
Test behavior
~~~~~~~~~~~~~

By default, the watcher mode runs tests alongside models via the ``dbt build`` command being executed by the producer ``DbtProducerWatcherOperator`` operator.

As a starting point, this execution mode does not support the ``TestBehavior.AFTER_EACH`` behavior, since the tests are not run as individual tasks. Since this is the default ``TestBehavior`` in Cosmos, we are injecting ``EmptyOperator`` as a starting point to ensure a seamless transition to the new mode.

The ``TestBehavior.BUILD`` behavior is embedded to the producer ``DbtProducerWatcherOperator`` operator.

The ``TestBehavior.NONE`` and ``TestBehavior.AFTER_ALL`` behave similarly to ``ExecutionMode.LOCAL``.

~~~~~~~~~~~~~~~~~~~~~~~~~~~
Airflow Datasets and Assets
~~~~~~~~~~~~~~~~~~~~~~~~~~~

While the ``ExecutionMode.WATCHER`` supports the ``emit_datasets`` parameter, the Airflow Datasets and Assets are emitted from the ``DbtProducerWatcherOperator`` task instead of the consumer tasks, as done for other Cosmos' execution modes.

~~~~~~~~~~~~~~~~~~~~~~
Source freshness nodes
~~~~~~~~~~~~~~~~~~~~~~

Since Cosmos 1.6, it `supports the rendering of source nodes <https://www.astronomer.io/blog/native-support-for-source-node-rendering-in-cosmos/>`_.

We noticed some Cosmos users use this feature alongside `overriding Cosmos source nodes <https://astronomer.github.io/astronomer-cosmos/configuration/render-config.html#customizing-how-nodes-are-rendered-experimental>`_ as sensors or another operator that allows them to skip the following branch of the DAG if the source is not fresh.

This use case is not currently supported by the ``ExecutionMode.WATCHER``, since the ``dbt build`` command does not run `source freshness checks <https://docs.getdbt.com/reference/commands/build#source-freshness-checks>`_.

We have a follow-up ticket to `further investigate this use case <https://github.com/astronomer/astronomer-cosmos/issues/2053>`_.


Additional details
-------------------

~~~~~~~~~~~~~~~~
Callback support
~~~~~~~~~~~~~~~~

The ``DbtProducerWatcherOperator`` and ``DbtConsumerWatcherSensor`` will use the user-defined callback function similar to ``ExecutionMode.LOCAL`` mode.

It is possible to define different ``callback`` behaviors for producer and consumer nodes by using ``setup_operator_args``, as described below.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Overriding ``operator_args``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``DbtProducerWatcherOperator`` and ``DbtConsumerWatcherSensor`` operators handle ``operator_args``  similar to the ``ExecutionMode.LOCAL`` mode.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Using Custom Args for the Producer and Watcher
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. versionadded:: 1.12.0

If you need to override ``operator_args`` for the ``DbtProducerWatcherOperator``, you can do so using ``setup_operator_args``.

When using ``ExecutionMode.WATCHER``, you may want to configure specific properties, such as ``retries`` specifically for the ``DbtProducerWatcherOperator`` task. This can be useful for several reasons:
- Improved resilience - transient issues (e.g., temporary database or network failures) can be automatically retried.
- Reduced manual intervention - failed producer runs can recover without requiring operator restarts.
- Better reliability - retry behaviour can be tuned independently from sensor tasks.

Example: Configure the producer task with custom retry settings.

.. code-block:: python

    from datetime import timedelta
    from cosmos.config import ExecutionConfig
    from cosmos.constants import ExecutionMode

    execution_config = ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
        setup_operator_args={
            "retries": 0,
            "retry_delay": timedelta(minutes=5),
        },
    )

This allows you to customize ``DbtProducerWatcherOperator`` retry behavior without affecting the arguments used by the other sensor tasks.

If configuring queues, we suggest using the previously mentioned ``watcher_dbt_execution_queue`` configuration instead of the ``setup_operator_args``.

.. note::
   Please note that ``setup_operator_args`` is specific to Cosmos and is not related to Airflow setup or teardown task.


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sensor slot allocation and polling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each ``DbtDag`` or ``DbtTaskGroup`` root node will startup during DAG runs  at - potentially - the same time as the DAG Run. This may not happen, since it is dependent on the
concurrency settings and available task slots in the Airflow deployment.

The consequence is that tasks may take longer to be updated if they are not sensing at the moment that the transformation happens.

We plan to review this behaviour and alternative approaches in the future.


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Asynchronous sensor execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Deferrable execution is currently supported only for dbt models, seeds and snapshots.
- Deferrable execution applies only to the first task attempt (try number 1). For subsequent retries, the sensor falls back to synchronous execution.

To disable asynchronous execution, set the ``deferrable`` flag to ``False`` in the ``operator_args``.

.. literalinclude:: ../../dev/dags/example_watcher.py
   :language: python
   :start-after: [START example_watcher_synchronous]
   :end-before: [END example_watcher_synchronous]

-------------------------------------------------------------------------------

Troubleshooting
---------------

Problem: "I changed from ``ExecutionMode.LOCAL`` to ``ExecutionMode.WATCHER``, but my DAG is running slower."
Answer: Please, check the number of threads that are being used by searching the producer task logs for a message similar to ``Concurrency: 1 threads (target='DEV')``. To leverage the Watcher mode, you should have a high number of threads, at least dbt's default of 4. Check the `dbt threading docs <https://docs.getdbt.com/docs/running-a-dbt-project/using-threads>`_ for more information on how to set the number of threads.


Summary
-------

``ExecutionMode.WATCHER`` represents a significant leap forward for running dbt in Airflow via Cosmos:

* ✅ Up to **5× faster** dbt DAG runs
* ✅ Maintains **model-level visibility** in Airflow
* ✅ Enables **smarter resource allocation**
* ✅ Built on proven Cosmos rendering techniques

This is an experimental feature, and we are looking for feedback from the community.

Stay tuned for further documentation and base image support for the ``ExecutionMode.WATCHER`` in upcoming releases.
