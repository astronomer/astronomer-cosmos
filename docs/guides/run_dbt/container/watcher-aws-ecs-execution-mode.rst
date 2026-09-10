.. _watcher-aws-ecs-execution-mode:

Watcher AWS ECS execution mode
==============================

.. versionadded:: 1.16.0

.. note::
   ``ExecutionMode.WATCHER_AWS_ECS`` is experimental.

``ExecutionMode.WATCHER_AWS_ECS`` combines the **speed of the** :ref:`watcher-execution-mode` **with the isolation of** :ref:`aws-container-run-job`.

This execution mode is for users who:

- Run dbt in ECS tasks today (``ExecutionMode.AWS_ECS``) and want one ``dbt build`` per Dag run instead of one container per node
- Run the ``ExecutionMode.WATCHER`` producer on an `Apache Airflow® <https://airflow.apache.org/>`_ worker whose memory is shared with other tasks, as on Amazon MWAA, and want the dbt process out of the worker

Background
~~~~~~~~~~

The :ref:`watcher-execution-mode` runs dbt as a single command while keeping model-level tasks in Airflow: a producer task runs ``dbt build`` and publishes the status of every node as dbt reports it; one consumer sensor per node waits for its status.

With ``ExecutionMode.WATCHER`` the producer runs dbt as a subprocess of the Airflow worker. With ``ExecutionMode.WATCHER_AWS_ECS`` the producer starts an ECS task, as ``ExecutionMode.AWS_ECS`` does, and reads dbt's JSON log lines from the task's CloudWatch log stream while it runs. The consumer sensors are unchanged.

For the watcher concept itself, refer to :ref:`watcher-execution-mode`.

How to use
~~~~~~~~~~

Users of ``ExecutionMode.AWS_ECS`` replace the ``execution_mode`` and add the CloudWatch log arguments:

.. code-block:: python

    from cosmos import DbtDag
    from cosmos.config import ExecutionConfig
    from cosmos.constants import ExecutionMode

    dag = DbtDag(
        dag_id="jaffle_shop_watcher_aws_ecs",
        # ... other DAG parameters ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER_AWS_ECS,
            dbt_project_path="dags/dbt/jaffle_shop",
        ),
        operator_args={
            "cluster": "dbt-cluster",
            "task_definition": "dbt-task",
            "container_name": "dbt",
            "awslogs_group": "/ecs/dbt",
            "awslogs_stream_prefix": "ecs/dbt",
            "awslogs_region": "eu-west-1",
        },
    )

**Key differences from** ``ExecutionMode.AWS_ECS``:

- The producer task runs the entire ``dbt build`` command in a single ECS task
- Consumer tasks (sensors) watch for the completion of their corresponding dbt models
- ``awslogs_group`` and ``awslogs_stream_prefix`` are mandatory: node statuses are read from that log stream

For the ECS task definition, IAM permissions and profile setup, refer to :ref:`aws-container-run-job`.

How statuses reach the consumers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ECS has no streaming log API, so the producer polls CloudWatch: every ``awslogs_fetch_interval`` (default 5 seconds for this producer; the provider default of 30 seconds is meant for displaying logs) it reads the new events of the task's log stream, parses each dbt JSON line and pushes the node statuses. After the task stops, it keeps polling until dbt's final ``CommandCompleted`` event has been read, or ``drain_timeout`` (default 60 seconds) has elapsed, so the last ``NodeFinished`` events are not lost.

The delay between a node finishing in dbt and its consumer seeing the status is therefore bounded by ``awslogs_fetch_interval`` plus the consumer's own ``poke_interval``.

Both arguments are set through ``operator_args``:

.. code-block:: python

    from datetime import timedelta

    operator_args = {
        # ... ECS and CloudWatch arguments ...
        "awslogs_fetch_interval": timedelta(seconds=10),
        "drain_timeout": timedelta(seconds=120),
    }

Test behavior
~~~~~~~~~~~~~

By default, ``ExecutionMode.WATCHER_AWS_ECS`` runs tests alongside models via the ``dbt build`` command executed by the producer task (``DbtProducerWatcherAwsEcsOperator``).

``TestBehavior.AFTER_EACH`` (the default) renders each model's tests as a ``DbtTestWatcherAwsEcsOperator``, a ``DbtConsumerWatcherAwsEcsSensor`` subclass that waits for the aggregated test status of its model; on retry it runs ``dbt test --select <model>`` in an ECS task.

``TestBehavior.AFTER_ALL`` renders a single ``DbtTestAwsEcsOperator`` that runs ``dbt test`` in a dedicated ECS task after all models complete.

``TestBehavior.NONE`` disables test tasks.

``TestBehavior.BUILD`` is not exposed: the build command is already executed by the producer task.

Known limitations
~~~~~~~~~~~~~~~~~

Amazon provider version
+++++++++++++++++++++++

Requires ``apache-airflow-providers-amazon >= 8.3.0``, the first release whose ``EcsRunTaskOperator`` and ``AwsTaskLogFetcher`` expose the hooks this producer builds on.

Producer arguments
++++++++++++++++++

- ``deferrable=True`` is rejected for the producer: the log stream is only read on the synchronous path, and the deferred path of ``EcsRunTaskOperator`` waits for the task without reading its logs. The producer holds a worker slot for the duration of the build, occupied by the CloudWatch polling, not by dbt. Consumer sensors defer as in the other watcher modes.
- ``reattach=True`` and ``wait_for_completion=False`` are rejected: the producer never re-runs or re-attaches to a dbt build. On retry it restores the statuses it had published and skips; consumers whose status is missing fall back to running their node in an ECS task.

Log delivery
++++++++++++

- The ``awslogs`` log driver of the dbt container must run in its default ``blocking`` mode. In ``non-blocking`` mode the driver drops lines under back-pressure, and a dropped ``NodeFinished`` line means a consumer that only fails when the producer has finished.
- The Airflow connection used for ``aws_conn_id`` needs ``logs:GetLogEvents`` on the log group, in addition to the ECS permissions ``ExecutionMode.AWS_ECS`` already requires.
- Lines the CloudWatch agent splits (events above 256 KB) are not parseable as JSON and are skipped; node status events are far below that limit.

Other inherited limitations
+++++++++++++++++++++++++++

The limitations of :ref:`watcher-execution-mode` and :ref:`aws-container-run-job` apply.
