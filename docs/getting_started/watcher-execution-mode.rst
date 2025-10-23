Introducing ``ExecutionMode.WATCHER``: Experimental High-Performance dbt Execution in Cosmos
===============================================================================

With the release of **Cosmos 1.11.0**, we are introducing a powerful new experimental execution mode — ``ExecutionMode.WATCHER`` — designed to drastically reduce dbt pipeline run times in Airflow.

Early benchmarks show that ``ExecutionMode.WATCHER`` can cut total DAG runtime **by up to 80%**, bringing performance **on par with running dbt CLI locally**.

-------------------------------------------------------------------------------

Background: The Problem with the Local Execution Mode in Cosmos
-----------------------------------------------------------

When running dbt via Cosmos using the default ``ExecutionMode.LOCAL``, each dbt model is executed as a separate Airflow task.

This provides strong observability and task-level retry control — but it comes at a cost. Each model runs a new dbt process, which introduces significant overhead.

Consider the `google/fhir-dbt-analytics <https://github.com/google/fhir-dbt-analytics>`_ project:

+--------------------------------------+----------------------------------+------------------+
| Run Type                             | Description                      | Total Runtime    |
+======================================+==================================+==================+
| Single ``dbt run`` (dbt CLI)             | Runs the whole DAG in one command | ~5m 30s          |
+--------------------------------------+----------------------------------+------------------+
| One ``dbt run`` per model, totalling 184 commands (dbt CLI) | Each model is its own task        | ~32m             |
+--------------------------------------+----------------------------------+------------------+

This difference motivated a rethinking of how Cosmos interacts with dbt.

-------------------------------------------------------------------------------

Concept: ``ExecutionMode.WATCHER``
----------------------------------

``ExecutionMode.WATCHER`` combines the **speed of a single dbt run** with the **observability and task management of Airflow**.

It is built on two operator types:

* **``DbtProducerWatcherOperator``** (`#1982 <https://github.com/astronomer/astronomer-cosmos/pull/1982>`_)
  Runs dbt **once** across the entire pipeline and sends model progress updates via Airflow **XComs**.

* **``DbtConsumerWatcherSensor``** (`#1998 <https://github.com/astronomer/astronomer-cosmos/pull/1998>`_)
  Watches those XComs and marks individual Airflow tasks as complete when their corresponding dbt models finish.

Together, these operators let you:

* Run dbt as a single command (for speed)
* Retain model-level observability (for clarity)
* Retry specific models (for resilience)

-------------------------------------------------------------------------------

Performance Gains
-----------------

Initial benchmarks illustrate significant improvements:

+---------------------------------------------+-----------+--------------------+
| Environment                                 | Threads   | Execution Time (s) |
+=============================================+===========+====================+
| dbt build (dbt CLI)                             | 4         | 6–7                |
+---------------------------------------------+-----------+--------------------+
| dbt run per model (local)                   | —         | 30                 |
+---------------------------------------------+-----------+--------------------+
| Cosmos LOCAL (Astro CLI)                    | —         | 10–15              |
+---------------------------------------------+-----------+--------------------+
| Cosmos WATCHER (Astro CLI)                  | 1         | 26                 |
|                                             | 2         | 14                 |
|                                             | 4         | 7                  |
|                                             | 8         | 4                  |
|                                             | 16        | 2                  |
+---------------------------------------------+-----------+--------------------+
| Cosmos WATCHER (Airflow / Astro Deployment) | 8         | ≈5                 |
+---------------------------------------------+-----------+--------------------+

The last line represents the performance improvement in a real-world Airflow deployment.

Depending on the dbt workflow topology, if your dbt DAG previously took 5 minutes with ``ExecutionMode.LOCAL``, you can expect it to complete in roughly **1 minute** with ``ExecutionMode.WATCHER``.

We plan to repeat these benchmarks and share the code with the community in the future.

-------------------------------------------------------------------------------

Example Usage of ``ExecutionMode.WATCHER``
------------------------------------------

There are two main ways to use the new execution mode in Cosmos — directly within a ``DbtDag``, or embedded as part of a ``DbtTaskGroup`` inside a larger DAG.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Example 1 — Using ``DbtDag`` with WATCHER Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can enable WATCHER mode directly in your ``DbtDag`` configuration.
This approach is best when your Airflow DAG is fully dedicated to a dbt project.

.. code-block:: python
   :caption: example_watcher.py
   :name: example_watcher

   example_watcher = DbtDag(
       # dbt/cosmos-specific parameters
       execution_config=ExecutionConfig(
           execution_mode=ExecutionMode.WATCHER,
       ),
       project_config=ProjectConfig(DBT_PROJECT_PATH),
       profile_config=profile_config,
       operator_args=operator_args,
       # normal dag parameters
       schedule="@daily",
       start_date=datetime(2023, 1, 1),
       catchup=False,
       dag_id="example_watcher",
       default_args={"retries": 0},
   )

As it can be observed, the only difference with the default ``ExecutionMode.LOCAL`` is the addition of the ``execution_config`` parameter with the ``execution_mode`` set to ``ExecutionMode.WATCHER``.

**How it works:**

* Cosmos executes your dbt project once via a producer task.
* Model-level Airflow tasks act as watchers, updating their state as dbt completes each model.
* The DAG remains fully observable and retryable, with **dramatically improved runtime performance** (often 5× faster than ``ExecutionMode.LOCAL``).

.. image:: /_static/jaffle_shop_watcher_dbt_dag_dag_run.png
    :alt: Cosmos DbtDag with `ExecutionMode.WATCHER`
    :align: center

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Example 2 — Using ``DbtTaskGroup`` with WATCHER Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

How retries work
................

When the ``dbt build`` command run by ``DbtProducerWatcherOperator`` fails, it will notify all the ``DbtConsumerWatcherSensor``.
The individual watcher tasks, that subclass ``DbtConsumerWatcherSensor``, will retry the dbt command by themselves using the same behaviour as ``ExecutionMode.LOCAL``.

Currently, we do not support retrying the ``DbtProducerWatcherOperator`` task itself.

This is a starting point and we consider implementing a more sophisticated retry mechanism in the future.


-------------------------------------------------------------------------------

Known Limitations
-------------------

These limitations will be revisited as the feature matures.

Installation of Airflow and dbt
...............................

The ``ExecutionMode.WATCHER`` works better when dbt and Airflow are installed in the same Python virtual environment, since it uses dbt `callback features <https://docs.getdbt.com/reference/programmatic-invocations#registering-callbacks>`_.
In case that is not possible, the producer task will only trigger the consumer tasks by the end of the execution, after it generated the ``run_results.json`` file.

We plan to improve this behaviour in the future by leveraging `dbt structured logging <https://docs.getdbt.com/reference/events-logging#structured-logging>`_.

Producer task implementation
............................

The producer task is implemented as a ``DbtProducerWatcherOperator`` operator and it currently relies on dbt being installed alongside the Airflow deployment, similar to the ``ExecutionMode.LOCAL`` implementation.
There are discussions about allowing this node to implemented as the ``ExecutionMode.VIRTUALEV`` and ``ExecutionMode.KUBERNETES`` execution modes, so that there is a higher isolationg between dbt and Airflow dependencies.

Individual dbt Operators
........................

While the efficiently implemented as part of ``ExecutionMode.WATCHER``:
* ``DbtSeedWatcherOperator``
* ``DbtSnapshotWatcherOperator``
* ``DbtRunWatcherOperator``

The ``DbtBuildWatcherOperator`` is not implemented, since the build command is executed by the producer ``DbtProducerWatcherOperator`` operator.

Even though the tests are being run as part of the producer task, the ``DbtTestWatcherOperator`` is currently implemented as a placeholder ``EmptyOperator``, and will be implemented as part of `#1974 <https://github.com/astronomer/astronomer-cosmos/issues/1974>`_.

Additionally, since the ``dbt build`` command does not run ``source`` nodes, the operator ``DbtSourceWatcherOperator`` is equivalent to the ``DbtSourceLocalOperator`` operator, from ``ExecutionMode.LOCAL``.

Finally, the following features are not implemented as operators under ``ExecutionMode.WATCHER``:

* ``dbt ls``
* ``dbt run-operation``
* ``dbt docs``
* ``dbt clone``

You can still invoke these operators using the default ``ExecutionMode.LOCAL`` mode.

Callback support
................

The ``DbtProducerWatcherOperator`` and ``DbtConsumerWatcherSensor`` will use the user-defined callback function similar to ``ExecutionMode.LOCAL`` mode.

This means that you can define a single callback function for all ``ExecutionMode.WATCHER`` tasks. The behaviour will be similar to the ``ExecutionMode.LOCAL`` mode, except that there will be unified ``results.txt`` file.

If there is demand, we will supporting different callback functions for the ``DbtProducerWatcherOperator`` and ``DbtConsumerWatcherSensor`` operators.


Overriding operator_args
........................

The ``DbtProducerWatcherOperator`` and ``DbtConsumerWatcherSensor`` operators handle ``operator_args``  similar to the ``ExecutionMode.LOCAL`` mode.

We plan to support different ``operator_args`` for the ``DbtProducerWatcherOperator`` and ``DbtConsumerWatcherSensor`` operators in the future.

Test behavior
..............

By default, the watcher mode runs tests alongside models via the ``dbt build`` command being executed by the producer ``DbtProducerWatcherOperator`` operator.

As a starting point, this execution mode does not support the ``TestBehavior.AFTER_EACH`` behaviour, since the tests are not run as individual tasks. Since this is the default ``TestBehavior`` in Cosmos, we are injecting ``EmptyOperator``s, a starting point, so the transition to the new mode can be seamless.

The ``TestBehavior.BUILD`` behaviour is embedded to the producer ``DbtProducerWatcherOperator`` operator.

Users can still use  the ``TestBehaviour.NONE`` and ``TestBehaviour.AFTER_ALL``.

Sensor slot allocation and polling
...................................

Each ``DbtDag`` or ``DbtTaskGroup`` root node will startup during DAG runs  at - potentially - the same time as the DAG Run. This may not happen, since it is dependent on the
concurrency settings and available task slots in the Airflow deployment.

The consequence is that tasks may take longer to be updated if they are not sensing at the moment that the transformation happens.

We plan to review this behaviour and alternative approaches in the future.


Synchronous sensor execution
............................

In Cosmos 1.11.0, the ``DbtConsumerWatcherSensor`` operator is implemented as a synchronous XCom sensor, which continuously occupies the worker slot - even if they're just sleeping and checking periodically.

An improvement is to change this behaviour and implement an asynchronous sensor execution, so that the worker slot is released until the condition, validated by the Airflow triggerer, is met.



-------------------------------------------------------------------------------


Summary
-------

``ExecutionMode.WATCHER`` represents a significant leap forward for running dbt in Airflow via Cosmos:

* ✅ Up to **5× faster** dbt DAG runs
* ✅ Maintains **model-level visibility** in Airflow
* ✅ Enables **smarter resource allocation**
* ✅ Built on proven Cosmos rendering techniques

This is an experimental feature and we are looking for feedback from the community.

Stay tuned for further documentation and base image support for WATCHER mode in upcoming releases.
