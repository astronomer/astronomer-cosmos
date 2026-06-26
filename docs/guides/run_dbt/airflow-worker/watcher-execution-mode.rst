.. _watcher-execution-mode:

Watcher execution mode (experimental)
=====================================

With the release of **Cosmos 1.11.0**, we are introducing a powerful new experimental execution mode — ``ExecutionMode.WATCHER`` — designed to drastically reduce dbt pipeline run times in `Apache Airflow® <https://airflow.apache.org/>`_.

Early benchmarks show that ``ExecutionMode.WATCHER`` can cut total DAG runtime **by up to 80%**, bringing performance **on par with running dbt CLI locally**. Since this execution mode improves the performance by leveraging `dbt threading <https://docs.getdbt.com/docs/running-a-dbt-project/using-threads>`_ and Airflow deferrable sensors, the performance gains will depend on three major factors:

- The amount of dbt ``threads`` set either via the dbt profile configuration or the dbt ``--threads`` flag
- The topology of the dbt pipeline
- The ``poke_interval`` and ``timeout`` settings of the ``DbtConsumerWatcherSensor`` operator, which determine the frequency and duration of the sensor's polling.


Background: The problem with the local execution mode in Cosmos
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


Concept: ``ExecutionMode.WATCHER``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ExecutionMode.WATCHER`` combines the **speed of a single dbt run** with the **observability and task management of Airflow**.

It is built on two operator types:

- ``DbtProducerWatcherOperator`` (`#1982 <https://github.com/astronomer/astronomer-cosmos/pull/1982>`_)
  Runs dbt **once** across the entire pipeline, register to `dbt event callbacks <https://docs.getdbt.com/reference/programmatic-invocations#registering-callbacks>`_ and sends model progress updates via Airflow **XComs**.

- ``DbtConsumerWatcherSensor`` (`#1998 <https://github.com/astronomer/astronomer-cosmos/pull/1998>`_)
  Watches those XComs and marks individual Airflow tasks as complete when their corresponding dbt models finish.

Together, these operators let you:

- Run dbt as a single command (for speed)
- Retain model-level observability (for clarity)
- Retry specific models (for resilience)


Performance gains
~~~~~~~~~~~~~~~~~

We used a dbt project developed by Google, the `google/fhir-dbt-analytics <https://github.com/google/fhir-dbt-analytics>`_ project, that interfaces with BigQuery. It contains:

- 2 seeds
- 52 sources
- 185 models

We benchmarked ``ExecutionMode.LOCAL`` against ``ExecutionMode.WATCHER`` on the project above using an Apache Airflow Helm chart deployment in May 2026. The headline results:

- **Runtime**: ``WATCHER`` ran the DAG about 41% faster than ``LOCAL`` at ``threads=8`` (roughly 5.2 versus 8.9 minutes). Even at dbt's default ``threads=4``, ``WATCHER`` was about 15% faster.
- **Memory**: Consumer pool peak memory dropped roughly 15% under ``WATCHER`` (10.0 GiB to about 8.5 GiB), because only the dedicated producer pod runs ``dbt build`` instead of one dbt process per concurrent consumer slot.
- **CPU**: ``WATCHER`` drives the consumer pool to roughly 90% of its available CPU via lightweight sensor work, whereas ``LOCAL`` plateaus at about 48% because each dbt task spends most of its time waiting on the warehouse. Producer CPU rises sub-linearly with threads (0.28 to 0.83 cores from ``threads=4`` to ``threads=16``).
- **Thread scaling**: ``threads=8`` is a strong default. Past 8 threads the producer ``dbt build`` keeps speeding up, but consumer sensors take proportionally longer to wake up (tracked in `astronomer/astronomer-cosmos#2657 <https://github.com/astronomer/astronomer-cosmos/issues/2657>`_), so total wall time plateaus.

For the full cluster setup, per-configuration tables, and methodology, see :ref:`watcher-benchmark`. The benchmark code and reproducible sweep recipe live in the `cosmos-benchmark project <https://github.com/astronomer/cosmos-benchmark>`_.

.. note::
   ``ExecutionMode.WATCHER`` relies on the ``threads`` value defined in your dbt profile. Start with a conservative value that matches the CPU capacity of your Airflow workers, then gradually increase it to find the sweet spot between faster runs and acceptable memory/CPU usage.

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



Example usage of ``ExecutionMode.WATCHER``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are two main ways to use the new execution mode in Cosmos — directly within a ``DbtDag``, or embedded as part of a ``DbtTaskGroup`` inside a larger DAG.

Example 1: Using ``DbtDag`` with ``ExecutionMode.WATCHER``
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

You can enable WATCHER mode directly in your ``DbtDag`` configuration.
This approach is best when your Airflow DAG is fully dedicated to a dbt project.

.. literalinclude:: ../../../../dev/dags/example_watcher.py
    :language: python
    :start-after: [START example_watcher]
    :end-before: [END example_watcher]

As it can be observed, the only difference with the default ``ExecutionMode.LOCAL`` is the addition of the ``execution_config`` parameter with the ``execution_mode`` set to ``ExecutionMode.WATCHER``. The ``ExecutionMode`` enum can be imported from ``cosmos.constants``. For more information on the ``ExecutionMode.LOCAL``, please, check the :ref:`local-execution` documentation.

**How it works:**

- Cosmos executes your dbt project once via a producer task.
- Model-level Airflow tasks act as watchers or sensors, updating their state as dbt completes each model.
- The DAG remains fully observable and retryable, with **dramatically improved runtime performance** (often 5× faster than ``ExecutionMode.LOCAL``).

**How it looks like:**

.. image:: /_static/jaffle_shop_watcher_dbt_dag_dag_run.png
    :alt: Cosmos DbtDag with `ExecutionMode.WATCHER`
    :align: center

Example 2: Using ``DbtTaskGroup`` with ``ExecutionMode.WATCHER``
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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

- Integrates seamlessly into complex Airflow DAGs.
- Uses the same high-performance producer/consumer execution model.
- Each ``DbtTaskGroup`` behaves independently — allowing modular dbt runs within larger workflows.

.. image:: /_static/jaffle_shop_watcher_dbt_taskgroup_dag_run.png
    :alt: Cosmos DbtDag with `ExecutionMode.WATCHER`
    :align: center


Additional details
~~~~~~~~~~~~~~~~~~

How retries work
++++++++++++++++

When the ``dbt build`` command run by ``DbtProducerWatcherOperator`` fails, it will notify all the ``DbtConsumerWatcherSensor``.

The individual watcher tasks that subclass ``DbtConsumerWatcherSensor`` can retry the dbt command themselves, using the same behavior as ``ExecutionMode.LOCAL``.

If a branch of the DAG fails, users can clear the status of a failed consumer task, including its downstream tasks, via the Airflow UI, and each of them will run in ``ExecutionMode.LOCAL``.

**Producer retry behavior**

.. versionchanged:: 1.13.0
   Producer no longer re-runs ``dbt build`` on retry: it returns success without re-executing
   (`#2283 <https://github.com/astronomer/astronomer-cosmos/pull/2283>`_).

.. versionchanged:: 1.14.1
   Producer raises ``AirflowSkipException`` instead of returning success, and backs up XCom values
   to an Airflow Variable so consumer sensors can read model statuses from the first attempt
   (`#2559 <https://github.com/astronomer/astronomer-cosmos/pull/2559>`_).

When the ``DbtProducerWatcherOperator`` is triggered for a retry (``try_number > 1``), it raises
``AirflowSkipException`` instead of re-running the ``dbt build`` command. Before skipping, it restores
XCom values from a backup so that consumer sensors can still read model statuses from the first attempt.

**XCom backup and restore:**

By default (``enable_watcher_reliable_retry = True``), each XCom push is incrementally backed up to an Airflow
Variable. This ensures that when the producer fails and Airflow clears XCom entries before the retry,
the backed-up values can be restored. On a successful run, the backup Variable is automatically deleted
to avoid stale data accumulating over time. These eager per-node writes can be deferred to a single
on-failure write to improve producer performance — see :ref:`producer-status-backup` below.

**How consumer retries work:**

1. The producer runs ``dbt build`` — some models succeed, some fail.
2. The producer task fails, and XCom values are backed up to a Variable.
3. On retry, the producer restores XCom from the Variable and raises ``AirflowSkipException``.
4. Consumer sensors read model statuses from the restored XCom.
5. Consumers for successful models complete immediately.
6. Consumers for failed models detect the error status and raise ``AirflowException``.
7. On their own retry, failed consumers fall back to running dbt individually for their model
   (via ``_fallback_to_non_watcher_run``), behaving like ``ExecutionMode.LOCAL``.

**Important considerations:**

- Users may configure ``retries`` freely on the producer task. On any retry attempt (``try_number > 1``),
  the producer gracefully skips execution — it will not re-run the ``dbt build`` command.
- Retrying the producer (or clearing an entire TaskGroup) is safe and will not cause duplicate dbt builds.
- During the retry of the sensor tasks, they will effectively run the corresponding dbt commands.

The overall retry behavior will be further improved once `#1978 <https://github.com/astronomer/astronomer-cosmos/issues/1978>`_ is implemented.

.. _producer-status-backup:

Producer status backup: reliability vs performance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 1.15.0

To let consumer sensors read model statuses after a producer retry, the producer must preserve the
per-node statuses it pushed on its first attempt (Airflow clears a task's XComs when it is retried).
The producer accumulates these statuses in an in-memory buffer during the run; the
``enable_watcher_reliable_retry`` configuration controls **when** that buffer is written to the Airflow
Variable that survives the retry:

- ``True`` (default) — the buffer is written to the Variable **eagerly, after every dbt node**, so the
  statuses survive any producer failure, including a hard ``SIGKILL``/OOM kill. Consumers never re-run
  dbt on a producer retry. This is the most reliable option, but the per-node Variable writes are
  measurable producer CPU/IO on large projects.
- ``False`` — the buffer is written to the Variable **once, when the producer is retried**, via the
  producer's retry callback (``on_retry_callback``). This removes the per-node Variable I/O and improves producer performance. A
  *graceful* producer failure (for example a dbt model error) still flushes the buffer, so consumers
  recover exactly as in the reliable mode. The trade-off: a *hard* kill (``SIGKILL``/OOM), where Airflow
  cannot run the callback, loses the in-memory statuses, so on the retry the affected consumer sensors
  fall back to running their dbt node locally. Results stay correct, but those transformations may run
  a second time.

.. list-table:: ``enable_watcher_reliable_retry`` comparison
   :header-rows: 1
   :widths: 40 30 30

   * - Behaviour
     - ``True`` (default)
     - ``False``
   * - Per-dbt-node Airflow Variable writes
     - Yes — one per node
     - No
   * - Producer CPU/IO overhead
     - Higher; scales with node count
     - Minimal
   * - When the backup Variable is written
     - Eagerly, after every node
     - Once, when the producer is retried (via ``on_retry_callback``)
   * - Recovers after a graceful producer failure (e.g. dbt error)
     - Yes
     - Yes
   * - Recovers after a hard kill (``SIGKILL``/OOM)
     - Yes
     - No — affected consumers re-run their dbt node
   * - Correct final DAG state
     - Yes
     - Yes

Set it via the ``[cosmos]`` section or the ``AIRFLOW__COSMOS__ENABLE_WATCHER_RELIABLE_RETRY`` environment
variable. Consider ``False`` for large dbt projects where the producer's per-node Variable I/O is a
bottleneck and occasional duplicate consumer runs after a hard producer kill are acceptable.

A future approach that delivers reliability and performance together is tracked in
`#2771 <https://github.com/astronomer/astronomer-cosmos/issues/2771>`_ (Airflow 3.3 Task & Asset
Store, AIP-103), which would let retries resume from the point of failure without the per-node
Variable backup.

Producer done gateway task (DbtTaskGroup only)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 1.14.1

When using ``DbtTaskGroup`` in watcher mode, the producer task may be skipped on retry
(via ``AirflowSkipException``). Because the producer is a leaf task inside the ``TaskGroup``,
Airflow's default ``trigger_rule="all_success"`` would cause any tasks downstream of the group
to be skipped as well — even when all consumer tasks succeeded. **This makes
``ExecutionMode.WATCHER`` behave differently from ``ExecutionMode.LOCAL``** when used with
``DbtTaskGroup``.

To prevent this (`#2594 <https://github.com/astronomer/astronomer-cosmos/issues/2594>`_),
Cosmos automatically adds an internal gateway task called
``dbt_producer_watcher_done`` inside the ``DbtTaskGroup``. This task:

- Sits directly downstream of the producer (``producer >> dbt_producer_watcher_done``)
- Uses ``trigger_rule="none_failed"`` so it succeeds even when the producer is skipped
- Absorbs the producer's skip state, preventing it from propagating to tasks downstream of the group

This gateway is only added for ``DbtTaskGroup`` — ``DbtDag`` does not need it because it handles
the producer-to-consumer dependency differently (``producer >> consumers`` with
``trigger_rule="always"``).

.. note::
   The ``dbt_producer_watcher_done`` task is visible in the Airflow UI. Click on it to see a
   description of its purpose in the "Doc" tab.

For a detailed history of how retry behavior has evolved across Cosmos releases, see
:ref:`watcher-retry-history`.

Watcher dbt Execution Queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 1.14.0

In watcher execution mode there are three different "types" of tasks that are executed:

- Producer tasks: execute a ``dbt build`` for the dbt project being rendered and orchestrated with watcher execution mode
- Consumer tasks: (first try) lightweight sensors that wait for the producer task to complete
- Consumer tasks: (retries) executes a dbt command for a specific node, **only when there is failure for that node**

Producer tasks typically require a high-memory worker to execute the ``dbt build`` command. On their first attempt, the consumer sensors require minimal CPU and memory resources. However, if these tasks retry, they execute the dbt command for the node, which may require significantly more resources.

Cosmos provides three independent queue settings to properly route each to the most appropriate worker:

.. _watcher-queue-configuration:

- ``watcher_dbt_producer_queue`` — the queue used by ``DbtProducerWatcherOperator`` tasks
- ``watcher_dbt_watcher_queue`` — the queue used by ``DbtConsumerWatcherSensor`` tasks on their initial (sensor) run
- ``watcher_dbt_retry_queue`` — the queue used by ``DbtConsumerWatcherSensor`` tasks on retries, when they execute the dbt command for a failed node

These settings enable you to:

- **Optimize resource allocation** — Use high-resource workers for producer tasks and sensor retries, and lightweight workers for initial sensor execution.
- **Improve scheduling efficiency** — Prevent resource contention between producer/retry executions and initial sensor tasks.
- **Scale independently** — Scale each queue separately based on its actual resource demand.

**Configuration:**

Set any combination of the three settings in your Airflow configuration:

.. code-block:: ini

   [cosmos]
   watcher_dbt_producer_queue = high_memory_queue
   watcher_dbt_watcher_queue = lightweight_queue
   watcher_dbt_retry_queue = high_memory_queue

Or via environment variables:

.. code-block:: bash

   export AIRFLOW__COSMOS__WATCHER_DBT_PRODUCER_QUEUE=high_memory_queue
   export AIRFLOW__COSMOS__WATCHER_DBT_WATCHER_QUEUE=lightweight_queue
   export AIRFLOW__COSMOS__WATCHER_DBT_RETRY_QUEUE=high_memory_queue

**How it works:**

- For watcher producer tasks (``DbtProducerWatcherOperator``), ``watcher_dbt_producer_queue`` is applied at task creation time.
- For watcher consumer tasks (``DbtConsumerWatcherSensor``), ``watcher_dbt_watcher_queue`` is applied at task creation time for the initial sensor run.
- For watcher consumer task retries, ``watcher_dbt_retry_queue`` is applied automatically via an `Airflow cluster policy <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/cluster-policies.html>`_ (``task_instance_mutation_hook``) that mutates ``task_instance.queue`` at runtime.

.. note::

  The Cosmos queue settings take priority over any ``queue`` set directly on the operator (e.g. via ``setup_operator_args``). The effective precedence for each task type is:

  - Producer: ``watcher_dbt_producer_queue`` > explicit ``queue`` (from ``setup_operator_args``) > ``operator_args`` > Airflow default queue
  - Consumer (initial run): ``watcher_dbt_watcher_queue`` > explicit ``queue`` > ``operator_args`` > Airflow default queue
  - Consumer (retries): ``watcher_dbt_retry_queue`` overrides the queue at runtime via the cluster policy

Installation of Apache Airflow® and dbt
++++++++++++++++++++++++++++++++++++++++

Since Cosmos 1.12.0, ``ExecutionMode.WATCHER`` works well regardless of whether dbt and Airflow are installed in the same Python virtual environment.

When dbt and Airflow are installed in the same Python virtual environment, the ``ExecutionMode.WATCHER`` uses dbt `callback features <https://docs.getdbt.com/reference/programmatic-invocations#registering-callbacks>`_.

When dbt and Airflow are not installed in the same Python virtual environment, the ``ExecutionMode.WATCHER`` consumes the dbt `structured logging <https://docs.getdbt.com/reference/events-logging#structured-logging>`_ to update the consumer tasks.

Synchronous versus Asynchronous sensor execution
++++++++++++++++++++++++++++++++++++++++++++++++

In Cosmos 1.11.0, the ``DbtConsumerWatcherSensor`` operator is implemented as a synchronous XCom sensor, which continuously occupies the worker slot - even if they're just sleeping and checking periodically.

Starting with Cosmos 1.12.0, the ``DbtConsumerWatcherSensor`` supports
`deferrable (asynchronous) execution <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>`_. Deferrable execution frees up the Airflow worker slot, while task status monitoring is handled by the Airflow triggerer component,
which increases overall task throughput. By default, the sensor now runs in deferrable mode.


Known limitations
~~~~~~~~~~~~~~~~~

Minimum supported dbt version
+++++++++++++++++++++++++++++

``ExecutionMode.WATCHER`` requires **dbt-core 1.5 or newer**. The watcher
relies on ``--log-format json`` to consume structured per-model events from
dbt, with the flag placed after the dbt subcommand (e.g.
``dbt build --log-format json …``). dbt-core 1.5 introduced finer-grained
log CLI parameters (see the
`dbt-core 1.5.0 changelog <https://github.com/dbt-labs/dbt-core/blob/v1.5.0/CHANGELOG.md#dbt-core-150---april-27-2023>`_,
"Implemented new log cli parameters for finer-grained control", tracked in
`dbt-core#6639 <https://github.com/dbt-labs/dbt-core/issues/6639>`_) and
accepts ``--log-format`` in that position; older dbt releases predate this
change and are not validated by Cosmos's watcher tests.

dbt versions older than 1.5 have all reached end of life and are no longer
supported by dbt Labs; see the
`dbt Core versions <https://docs.getdbt.com/docs/dbt-versions/core>`_ page
for current dbt-Labs-supported releases. If you must run an older dbt
release with Cosmos, use ``ExecutionMode.LOCAL`` instead.

Producer task implementation
++++++++++++++++++++++++++++

The producer task is implemented as a ``DbtProducerWatcherOperator`` and currently relies on dbt being installed alongside the Airflow deployment, as in the ``ExecutionMode.LOCAL`` implementation.

The alternative to this implementation is to use ``ExecutionMode.WATCHER_KUBERNETES``, which is built on top of ``ExecutionMode.KUBERNETES``. Check :ref:`watcher-kubernetes-execution-mode` for more information.

Individual dbt Operators
++++++++++++++++++++++++

The ``ExecutionMode.WATCHER`` efficiently implements the following operators:
- ``DbtSeedWatcherOperator``
- ``DbtSnapshotWatcherOperator``
- ``DbtRunWatcherOperator``

However, other operators that are available in the ``ExecutionMode.LOCAL`` mode are not implemented.

The ``DbtBuildWatcherOperator`` is not implemented, since the build command is executed by the producer ``DbtProducerWatcherOperator`` operator.

Additionally, since the ``dbt build`` command does not run ``source`` nodes, the operator ``DbtSourceWatcherOperator`` is equivalent to the ``DbtSourceLocalOperator`` operator, from ``ExecutionMode.LOCAL``.

Finally, the following features are not implemented as operators under ``ExecutionMode.WATCHER``:

- ``dbt ls``
- ``dbt run-operation``
- ``dbt docs``
- ``dbt clone``

You can still invoke these operators using the default ``ExecutionMode.LOCAL`` mode.

Test behavior
+++++++++++++

By default, the watcher mode runs tests alongside models via the ``dbt build`` command being executed by the producer ``DbtProducerWatcherOperator`` operator.

.. versionchanged:: 1.14.0

Starting with Cosmos 1.14.0, ``TestBehavior.AFTER_EACH`` is fully supported in ``ExecutionMode.WATCHER``.
Each test task is rendered as a ``DbtTestWatcherOperator`` (a ``DbtConsumerWatcherSensor`` subclass) that watches
the aggregated test results published by the producer via XCom. This means test tasks now behave as real sensors
rather than no-op placeholders.

In Cosmos versions prior to 1.14.0, ``TestBehavior.AFTER_EACH`` was not supported by the watcher mode because tests
were not run as individual tasks. Since ``TestBehavior.AFTER_EACH`` is the default ``TestBehavior`` in Cosmos,
``EmptyOperator`` tasks were injected as placeholders to ensure a seamless transition to the new mode.

The ``TestBehavior.BUILD`` behavior is embedded in the producer ``DbtProducerWatcherOperator`` operator.

The ``TestBehavior.NONE`` and ``TestBehavior.AFTER_ALL`` behave similarly to ``ExecutionMode.LOCAL``.

Apache Airflow® Datasets and Assets
++++++++++++++++++++++++++++++++++++

.. versionchanged:: 1.14.0

The ``ExecutionMode.WATCHER`` supports the ``emit_datasets`` parameter. Starting with Cosmos 1.14.0,
each **consumer** sensor task emits the Airflow Dataset (Asset) for the dbt model it watches, matching
the per-model granularity of other execution modes. The **producer** task does not emit datasets.

For details on URI patterns and how dataset emission differs between execution modes, see
:ref:`data-aware-scheduling`.

Source freshness nodes
++++++++++++++++++++++

Since Cosmos 1.6, it `supports the rendering of source nodes <https://www.astronomer.io/blog/native-support-for-source-node-rendering-in-cosmos/>`_.

Starting with Cosmos 1.14.0, ``ExecutionMode.WATCHER`` supports source freshness aware execution. When
``source_rendering_behavior`` is not ``NONE``, the producer task automatically runs ``dbt source freshness``
before ``dbt build``, and the freshness callback determines which dependent nodes are skipped based on
stale sources. See :ref:`watcher-source-freshness` for details.

Concurrent DAG runs with ``depends_on_past``
++++++++++++++++++++++++++++++++++++++++++++

When ``depends_on_past=True`` is used together with concurrent DAG runs, a race can occur between consecutive runs where the next run's producer starts while the previous run's consumer fallback is still executing, causing two dbt processes to write to the same models concurrently.

As a workaround, set ``max_active_runs=1`` on the DAG.

For details, see `#2596 <https://github.com/astronomer/astronomer-cosmos/issues/2596>`_.


Advanced config
~~~~~~~~~~~~~~~

Callback support
++++++++++++++++

The ``DbtProducerWatcherOperator`` and ``DbtConsumerWatcherSensor`` will use the user-defined callback function similar to ``ExecutionMode.LOCAL`` mode.

You can define different ``callback`` behaviors for producer and consumer nodes by using ``operator_args`` to configure the consumer callback and ``setup_operator_args`` to override the callback for the producer, as described below.

.. _watcher-source-freshness:

Source freshness aware execution (Experimental)
++++++++++++++++++++++++++++++++++++++++++++++++

.. versionadded:: 1.14.0

.. warning::

   This feature is **experimental** and may change without a deprecation period.

When ``source_rendering_behavior`` is set to ``ALL`` or ``WITH_TESTS_OR_FRESHNESS`` in ``RenderConfig``,
the producer automatically runs ``dbt source freshness`` before ``dbt build`` and always invokes the
freshness callback afterward. The callback inspects the freshness results (``sources_json``) and returns
a list of ``(unique_id, state)`` tuples for any nodes that should be pre-marked; it may return an empty
list when no nodes need special handling. Each returned node receives a pre-populated XCom entry; nodes
returned with a non-success state are also added to ``--exclude`` so dbt skips them entirely.

The consumer sensor recognises three state families: ``"skipped"`` raises ``AirflowSkipException``,
``"success"`` / ``"pass"`` / ``"warn"`` marks the task as succeeded, and anything else (e.g.
``"failed"``, ``"error"``) raises ``AirflowException``.  The default callback always returns
``"skipped"`` for stale dependents — a node is skipped only when **all** of its upstream dependencies
are stale or already skipped.

.. literalinclude:: ../../../../dev/dags/watcher_with_freshness_check.py
    :language: python
    :start-after: [START example_watcher_with_freshness]
    :end-before: [END example_watcher_with_freshness]

To override the default logic, pass a ``freshness_callback`` via ``setup_operator_args``
(custom callback support added in Cosmos 1.15.0):

.. code-block:: python

    def my_freshness_callback(
        context: Context,
        dag: Any,
        task_group: TaskGroup | None,
        nodes: dict[str, DbtNode] | None,  # full DbtGraph.nodes for dependency traversal
        sources_json: dict[str, Any] | None,  # parsed target/sources.json
    ) -> list[tuple[str, str]]:  # (unique_id, state) pairs
        ...


    execution_config = ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
        setup_operator_args={"freshness_callback": my_freshness_callback},
    )

**Known limitations:**

- Incompatible with ``selector`` in ``RenderConfig`` — ``--exclude`` is ignored by dbt when a YAML selector is active.
- ``dbt source freshness`` is always re-executed at runtime; ``LoadMode.DBT_MANIFEST`` freshness data is not consulted.
- Not supported for ``ExecutionMode.WATCHER_KUBERNETES``.


Overriding ``operator_args``
++++++++++++++++++++++++++++

The ``DbtProducerWatcherOperator`` and ``DbtConsumerWatcherSensor`` operators handle ``operator_args``  similar to the ``ExecutionMode.LOCAL`` mode.

Using Custom Args for the Producer and Watcher
++++++++++++++++++++++++++++++++++++++++++++++
.. versionadded:: 1.12.0

If you need to override ``operator_args`` for the ``DbtProducerWatcherOperator``, you can do so using ``setup_operator_args``.

When using ``ExecutionMode.WATCHER``, you may want to configure specific properties, such as ``retries`` specifically for the ``DbtProducerWatcherOperator`` task. This can be useful for several reasons:
- Improved resilience - transient issues (e.g., temporary database or network failures) can be automatically retried.
- Reduced manual intervention - failed producer runs can recover without requiring operator restarts.
- Better reliability - retry behavior can be tuned independently from sensor tasks.

Because the producer gracefully skips re-execution on retries (returning success without re-running the dbt build), it is safe to set ``retries`` to any value you wish. Currently, retries work for sensor tasks, which, after a first failed attempt, will effectively run the corresponding dbt command.

Example: Configure the producer task with custom retry settings.

.. code-block:: python

    from datetime import timedelta
    from cosmos.config import ExecutionConfig
    from cosmos.constants import ExecutionMode

    execution_config = ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
        setup_operator_args={
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
        },
    )

This allows you to customize ``DbtProducerWatcherOperator`` retry behavior without affecting the arguments used by the other sensor tasks.

If configuring queues, we suggest using the previously mentioned ``watcher_dbt_producer_queue``, ``watcher_dbt_watcher_queue``, and ``watcher_dbt_retry_queue`` configurations instead of ``setup_operator_args``.

.. note::
   Please note that ``setup_operator_args`` is specific to Cosmos and is not related to Airflow setup or teardown task.


Sensor slot allocation and polling
++++++++++++++++++++++++++++++++++

Each ``DbtDag`` or ``DbtTaskGroup`` root node will startup during DAG runs  at - potentially - the same time as the DAG Run. This may not happen, since it is dependent on the
concurrency settings and available task slots in the Airflow deployment.

The consequence is that tasks may take longer to be updated if they are not sensing at the moment that the transformation happens.

We plan to review this behaviour and alternative approaches in the future.


Asynchronous sensor execution
+++++++++++++++++++++++++++++

- Deferrable execution is currently supported only for dbt models, seeds and snapshots.
- Deferrable execution applies only to the first task attempt (try number 1). For subsequent retries, the sensor falls back to synchronous execution.

To disable asynchronous execution, set the ``deferrable`` flag to ``False`` in the ``operator_args``.

.. literalinclude:: ../../../../dev/dags/example_watcher.py
   :language: python
   :start-after: [START example_watcher_synchronous]
   :end-before: [END example_watcher_synchronous]


Troubleshooting
~~~~~~~~~~~~~~~

Problem: "I changed from ``ExecutionMode.LOCAL`` to ``ExecutionMode.WATCHER``, but my DAG is running slower."
Answer: Please, check the number of threads that are being used by searching the producer task logs for a message similar to ``Concurrency: 1 threads (target='DEV')``. To leverage the Watcher mode, you should have a high number of threads, at least dbt's default of 4. Check the `dbt threading docs <https://docs.getdbt.com/docs/running-a-dbt-project/using-threads>`_ for more information on how to set the number of threads.

Problem: "I cannot see dbt debug logs in the producer task."
Answer: When ``ExecutionMode.WATCHER`` runs dbt as a subprocess (i.e. when dbt and Airflow are **not** installed in the same Python virtual environment), pass ``--debug`` via ``dbt_cmd_global_flags`` in ``operator_args``:

.. code-block:: python

   operator_args = {
       "install_deps": True,
       "dbt_cmd_global_flags": ["--debug"],
   }

This causes dbt debug messages to be forwarded to Python's ``logging`` module at the ``DEBUG`` level. These messages will only be visible if Airflow's logging level is configured to ``DEBUG`` (or more verbose). To make dbt debug logs visible, set:

.. code-block:: bash

   export AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG

Or add the equivalent to your ``airflow.cfg``:

.. code-block:: ini

   [logging]
   logging_level = DEBUG

.. note::
   Enabling ``DEBUG``-level logging is verbose and may significantly increase log volume. Consider scoping it to development or troubleshooting environments rather than production deployments.


Summary
~~~~~~~

``ExecutionMode.WATCHER`` represents a significant leap forward for running dbt in Airflow via Cosmos:

- ✅ Up to **5× faster** dbt DAG runs
- ✅ Maintains **model-level visibility** in Airflow
- ✅ Enables **smarter resource allocation**
- ✅ Built on proven Cosmos rendering techniques

This is an experimental feature, and we are looking for feedback from the community.

Stay tuned for further documentation and base image support for the ``ExecutionMode.WATCHER`` in upcoming releases.
