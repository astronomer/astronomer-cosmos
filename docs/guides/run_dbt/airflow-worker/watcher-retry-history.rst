:orphan:

.. _watcher-retry-history:

Watcher Retry Behaviour History
-------------------------------

This page documents the evolution of retry behaviour in ``ExecutionMode.WATCHER`` across Cosmos releases.

Goals
+++++

* The Airflow DAG status should match the dbt pipeline status, whether successful or failed
* Users should be able to retry individual tasks via Airflow retry
* Users should be able to retry the whole DAG via Airflow automatic retry — so humans do not need to intervene when the DAG fails
* Users should be able to retry the whole DAG via Airflow clear
* Avoid duplicate or concurrent runs of the same dbt transformation in the same DAG run

Does the DAG status match dbt's?
++++++++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Outcome
   * - **1.11.0**
     - **Yes**.
   * - **1.11.1**
     - Same as 1.11.0.
   * - **1.11.2**
     - Same as 1.11.0.
   * - **1.11.3**
     - Same as 1.11.0.
   * - **1.12.0**
     - Same as 1.11.0.
   * - **1.12.1**
     - Same as 1.11.0.
   * - **1.13.0**
     - **Maybe**. Yes if successful in the first run. No if retries happen, unless users manually
       clear the producer task.
   * - **1.13.1**
     - Same as 1.13.0.
   * - **1.14.0**
     - **No** — on producer retry, dbt model failures from the first attempt are silently dropped.
       The consumer tasks for those models are marked successful instead of running their fallback
       retry, so the DAG appears successful even though dbt failed.
   * - **1.14.1**
     - **Yes**.

Task-level retry — consumer
+++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behaviour
   * - **1.11.0**
     - Fallback to ``ExecutionMode.LOCAL`` behavior (``dbt run --select <model>``).
   * - **1.11.1**
     - Same as 1.11.0.
   * - **1.11.2**
     - Same as 1.11.0.
   * - **1.11.3**
     - Same as 1.11.0.
   * - **1.12.0**
     - Similar to 1.11.0. Fixes rendering of dbt compiled SQL as a templated field
       (`#2209 <https://github.com/astronomer/astronomer-cosmos/pull/2209>`_); consumers run
       asynchronously when they behave as sensors
       (`#2084 <https://github.com/astronomer/astronomer-cosmos/pull/2084>`_), letting them detect
       producer failure faster and freeing worker slots sooner.
   * - **1.12.1**
     - Same as 1.12.0.
   * - **1.13.0**
     - Same as 1.12.0.
   * - **1.13.1**
     - Same as 1.12.0.
   * - **1.14.0**
     - Similar to 1.11.0. Affected by an Airflow limitation
       (`#2554 <https://github.com/astronomer/astronomer-cosmos/issues/2554>`_): because the producer
       returns success on retry and Airflow does not preserve XCom across retries, consumers lose
       the model statuses from the first attempt and may silently mark failed models as successful.
   * - **1.14.1**
     - Similar to 1.11.0. Consumers always read correct model statuses thanks to the producer's
       XCom backup mechanism — see *Task-level retry — producer*.

Task-level retry — producer
+++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behaviour
   * - **1.11.0**
     - Relaunches the entire ``dbt build`` — dangerous duplicate/concurrent run.
   * - **1.11.1**
     - Same as 1.11.0.
   * - **1.11.2**
     - Manual clear still relaunches ``dbt build``. Auto-retry is now blocked because Cosmos forces
       producer ``retries`` to ``0`` (see *Automatic retries*).
   * - **1.11.3**
     - Same as 1.11.2.
   * - **1.12.0**
     - Same as 1.11.2.
   * - **1.12.1**
     - Same as 1.12.0.
   * - **1.13.0**
     - Producer returns success on ``try_number > 1``
       (`#2283 <https://github.com/astronomer/astronomer-cosmos/pull/2283>`_) — logs an informational
       message and does not re-run ``dbt build``. Also fixes empty/ephemeral models hanging
       consumers (`#2279 <https://github.com/astronomer/astronomer-cosmos/pull/2279>`_). Only
       reachable via manual clear, since ``retries`` is still forced to ``0``.
   * - **1.13.1**
     - Same as 1.13.0.
   * - **1.14.0**
     - Producer returns success on retry without re-running ``dbt build``.
   * - **1.14.1**
     - Producer raises ``AirflowSkipException`` on retry
       (`#2559 <https://github.com/astronomer/astronomer-cosmos/pull/2559>`_) instead of returning
       success — explicit "skipped" state rather than misleadingly "successful". The producer also
       backs up its XCom state (model statuses) to an Airflow Variable during execution and
       restores it on retry, so consumers always read correct model statuses. The backup Variable
       is cleaned up on success.

       **Known issues with the XCom backup mechanism:**

       * `#2619 <https://github.com/astronomer/astronomer-cosmos/issues/2619>`_ — backup Variable
         key is not sanitized for ``:`` and ``+`` in Airflow 3 default ``run_id`` formats; strict-naming
         secrets backends (e.g. GCP / AWS Secret Manager) reject the name, breaking every
         ``Variable.get`` / ``Variable.set`` from the producer.
       * `#2625 <https://github.com/astronomer/astronomer-cosmos/issues/2625>`_ — on Airflow 2,
         ``_get_task_group_id()`` returns ``None``, so multiple ``DbtTaskGroup`` producers in the
         same DAG run share one backup key and log ``UniqueViolation`` on every model completion.

Automatic retries
+++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behaviour
   * - **1.11.0**
     - No safeguard; producer auto-retries would relaunch ``dbt build``, while consumer tasks may
       be running their own retries.
   * - **1.11.1**
     - Same as 1.11.0.
   * - **1.11.2**
     - Producer ``retries`` forced to ``0`` by Cosmos — no auto-retry possible on the producer.
   * - **1.11.3**
     - Same as 1.11.2.
   * - **1.12.0**
     - Same as 1.11.2.
   * - **1.12.1**
     - Same as 1.11.2.
   * - **1.13.0**
     - Same as 1.11.2.
   * - **1.13.1**
     - Same as 1.11.2.
   * - **1.14.0**
     - Forced ``retries=0`` on the producer is removed
       (`#2479 <https://github.com/astronomer/astronomer-cosmos/pull/2479>`_), fixing
       `#2429 <https://github.com/astronomer/astronomer-cosmos/issues/2429>`_. Producer auto-retries
       return success without re-running ``dbt build``, but Airflow does not preserve XCom across
       retries (`#2554 <https://github.com/astronomer/astronomer-cosmos/issues/2554>`_), so failed
       dbt models can be silently marked successful.
   * - **1.14.1**
     - Producer auto-retries raise ``AirflowSkipException``; XCom is restored from the Variable
       backup so consumers read correct model statuses. Subject to the XCom backup known issues
       — see *Task-level retry — producer*.

Full DAG / TaskGroup clear
++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behaviour
   * - **1.11.0**
     - Relaunches the entire ``dbt build`` — dangerous duplicate/concurrent run.
   * - **1.11.1**
     - Same as 1.11.0.
   * - **1.11.2**
     - Same as 1.11.0.
   * - **1.11.3**
     - Same as 1.11.0.
   * - **1.12.0**
     - Same as 1.11.0.
   * - **1.12.1**
     - Same as 1.11.0.
   * - **1.13.0**
     - Producer returns success on retry without re-running ``dbt build``; consumers run using
       ``ExecutionMode.LOCAL``. Works correctly on a manual full clear.
   * - **1.13.1**
     - Same as 1.13.0.
   * - **1.14.0**
     - Same as 1.13.0, but Airflow does not preserve XCom across retries
       (`#2554 <https://github.com/astronomer/astronomer-cosmos/issues/2554>`_), so failed dbt
       models can be silently marked successful.
   * - **1.14.1**
     - Producer raises ``AirflowSkipException`` (skipped, not successful); XCom is restored from
       the Variable backup so consumers read correct model statuses (subject to the XCom backup
       known issues — see *Task-level retry — producer*). For ``DbtTaskGroup``, a gateway task
       ``dbt_producer_watcher_done``
       (`#2597 <https://github.com/astronomer/astronomer-cosmos/pull/2597>`_) with
       ``trigger_rule="none_failed"`` is added downstream of the producer to absorb its skip state
       so it does not propagate to tasks downstream of the group
       (`#2594 <https://github.com/astronomer/astronomer-cosmos/issues/2594>`_). The gateway is
       only added for ``DbtTaskGroup`` — ``DbtDag`` does not need it.

Avoid duplicate or concurrent runs of the same dbt transformation in the same DAG run
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behaviour
   * - **1.11.0**
     - **Not met.** Producer auto-retry, manual clear, or full DAG/TaskGroup clear relaunches
       ``dbt build`` and re-runs all transformations — potentially in parallel with consumer
       Fallback to ``ExecutionMode.LOCAL`` behavior runs of the same models.
   * - **1.11.1**
     - Same as 1.11.0.
   * - **1.11.2**
     - **Not met.** Producer ``retries`` forced to ``0`` — auto-re-run is impossible. Manual
       producer clear or full DAG/TaskGroup clear still relaunches ``dbt build`` and may run
       concurrently with consumer fallbacks.
   * - **1.11.3**
     - Same as 1.11.2.
   * - **1.12.0**
     - Same as 1.11.2.
   * - **1.12.1**
     - Same as 1.11.2.
   * - **1.13.0**
     - **Not met.** Producer returns success on retry without re-running ``dbt build``, so retries
       and full clears no longer relaunch the entire build. However, when a consumer sensor times
       out and Airflow auto-retries it, the consumer's ``ExecutionMode.LOCAL`` fallback runs
       unconditionally without checking whether the producer is still running — which can cause
       concurrent runs of the same transformation. Fixed in 1.14.1 by
       `#2592 <https://github.com/astronomer/astronomer-cosmos/pull/2592>`_.
   * - **1.13.1**
     - Same as 1.13.0.
   * - **1.14.0**
     - Same as 1.13.0 — forced ``retries=0`` is lifted, but the consumer-sensor-retry concurrent
       run risk persists. Fixed in 1.14.1 by
       `#2592 <https://github.com/astronomer/astronomer-cosmos/pull/2592>`_.
   * - **1.14.1**
     - **Met.** Producer raises ``AirflowSkipException`` on retry — no ``dbt build`` re-run. On
       consumer sensor retry (`#2592 <https://github.com/astronomer/astronomer-cosmos/pull/2592>`_),
       Cosmos now checks the producer's state first: if it is still running, the sensor keeps
       polling instead of launching a duplicate ``dbt`` invocation; only after the producer reaches
       a terminal state does the consumer fall back to ``ExecutionMode.LOCAL``.
