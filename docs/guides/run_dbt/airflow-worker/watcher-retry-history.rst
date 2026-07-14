:orphan:

.. _watcher-retry-history:

Watcher retry behavior history
------------------------------

While ``ExecutionMode.WATCHER`` can significantly improve DAG run times, it is based on
non-idempotent `Apache Airflow® <https://airflow.apache.org/>`_ tasks and relies on a complex retry
mechanism in which one task's status can affect another task's status. This is the reason
``ExecutionMode.WATCHER`` remained marked as experimental for several months — until this retry
behavior was hardened. With Cosmos 1.15.0, ``ExecutionMode.WATCHER`` is marked stable. This document
aims to present how each aspect of retries has evolved within the Cosmos watcher implementation
across Cosmos releases.

Goals
+++++

- The Airflow ``DbtDag`` / ``DbtTaskGroup`` state should match the dbt pipeline status, whether successful or failed
- Users should be able to retry individual tasks via Airflow retry
- Users should be able to retry the whole DAG via Airflow automatic retry — so humans do not need to intervene when the DAG fails
- Users should be able to retry the whole DAG via Airflow clear
- Avoid duplicate or concurrent runs of the same dbt transformation in the same DAG run

Does the Airflow state match dbt's?
+++++++++++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Outcome
   * - **1.11.0**
     - **Yes**.
   * - **1.11.1**
     - **Yes**. Same as 1.11.0.
   * - **1.11.2**
     - **Yes**. Same as 1.11.0.
   * - **1.11.3**
     - **Yes**. Same as 1.11.0.
   * - **1.12.0**
     - **Yes**. Same as 1.11.0.
   * - **1.12.1**
     - **Yes**. Same as 1.11.0.
   * - **1.13.0**
     - **Maybe**. Yes if successful in the first run. No if retries happen, unless users manually
       clear the producer task.
   * - **1.13.1**
     - **Maybe**. Same as 1.13.0.
   * - **1.14.0**
     - **No** — on producer retry, dbt model failures from the first attempt are silently dropped.
       The consumer tasks for those models are marked successful instead of running their fallback
       retry, so the DAG appears successful even though dbt failed.
   * - **1.14.1**
     - **Yes**.
   * - **1.14.2**
     - **Yes**. Same as 1.14.1, plus a previously unreported "false green" gap is closed:
       when a model fails on its first attempt and dbt skips downstream nodes via the
       ``upstream_failure`` path, those downstream nodes are now rewritten from SKIPPED to
       **failed** instead of being silently marked successful. The DAG therefore fails rather
       than completing ``success`` with the tables un-materialized, and Airflow's automatic
       retries re-run the affected models via consumer fallback — so the whole DAG recovers on
       retry without any manual intervention
       (`#2684 <https://github.com/astronomer/astronomer-cosmos/pull/2684>`_).
   * - **1.15.0**
     - **Yes**. Same as 1.14.2 for both values of ``enable_watcher_reliable_retry`` — see
       *Task-level retry — producer*. With ``False`` a hard producer kill can make some consumers
       re-run a transformation, but the final DAG state still matches dbt's
       (`#2776 <https://github.com/astronomer/astronomer-cosmos/issues/2776>`_).

Task-level retry — consumer
+++++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behavior
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
     - Similar to 1.12.0. Affected by an Airflow limitation
       (`#2554 <https://github.com/astronomer/astronomer-cosmos/issues/2554>`_): because the producer
       returns success on retry and Airflow does not preserve XCom across retries, consumers lose
       the model statuses from the first attempt and may silently mark failed models as successful.
   * - **1.14.1**
     - Similar to 1.12.0. Consumers always read correct model statuses thanks to the producer's
       XCom backup mechanism — see *Task-level retry — producer*.
   * - **1.14.2**
     - Similar to 1.14.1, with two additions. First, consumers for models that dbt marked as
       skipped due to upstream failure (``upstream_failure``) now fail on attempt 1 rather than
       skipping — Cosmos rewrites those ``"skipped"`` statuses to ``"failed"`` in the
       producer-side log parser, so Airflow retries and the existing
       ``_fallback_to_non_watcher_run`` path runs the model locally once its upstream has
       recovered (`#2684 <https://github.com/astronomer/astronomer-cosmos/pull/2684>`_).
       Second, the consumer's fallback ``dbt`` invocation no longer inherits the producer's
       internal ``--log-format json`` flag, so retry task logs default to dbt's normal text
       output instead of one structured event per line
       (`#2713 <https://github.com/astronomer/astronomer-cosmos/pull/2713>`_); users who want
       JSON output on retry can opt in via
       ``operator_args={"dbt_cmd_flags": ["--log-format", "json"]}``.
   * - **1.15.0**
     - Same as 1.14.2. With ``enable_watcher_reliable_retry=False`` only a *hard* producer kill
       (``SIGKILL``/OOM) leaves consumers without restored statuses, so they fall back to running
       their dbt node; a graceful producer failure still restores them — see
       *Task-level retry — producer*.

Task-level retry — producer
+++++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behavior
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
     - Same as 1.11.2.
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

       **Known issues with the XCom backup mechanism (both fixed in 1.14.2):**

       - `#2619 <https://github.com/astronomer/astronomer-cosmos/issues/2619>`_ — backup Variable
         key is not sanitized for ``:`` and ``+`` in Airflow 3 default ``run_id`` formats; strict-naming
         secrets backends (e.g. GCP Secret Manager / AWS Secrets Manager) reject the name, breaking every
         ``Variable.get`` / ``Variable.set`` from the producer.
       - `#2625 <https://github.com/astronomer/astronomer-cosmos/issues/2625>`_ — on Airflow 2,
         ``_get_task_group_id()`` returns ``None``, so multiple ``DbtTaskGroup`` producers in the
         same DAG run share one backup key and log ``UniqueViolation`` on every model completion.
   * - **1.14.2**
     - Same as 1.14.1, with both XCom-backup known issues from 1.14.1 fixed. The key generator now
       sanitizes every component to ``[A-Za-z0-9_-]`` — the safest subset across the secrets
       backends Airflow ships with — so ``:`` / ``+`` from default Airflow 3 ``run_id`` formats no
       longer break strict-naming backends
       (`#2629 <https://github.com/astronomer/astronomer-cosmos/pull/2629>`_, closes
       `#2619 <https://github.com/astronomer/astronomer-cosmos/issues/2619>`_). And
       ``_get_task_group_id`` now reads ``task.task_group.group_id`` (the correct attribute) instead
       of the non-existent ``task.task_group_id`` it was reading before, so each ``DbtTaskGroup``
       producer in a DAG writes to a distinct backup Variable
       (`#2683 <https://github.com/astronomer/astronomer-cosmos/pull/2683>`_, closes
       `#2625 <https://github.com/astronomer/astronomer-cosmos/issues/2625>`_).
   * - **1.15.0**
     - Same as 1.14.2 by default. A new ``enable_watcher_reliable_retry`` configuration
       (`#2776 <https://github.com/astronomer/astronomer-cosmos/issues/2776>`_) controls *when* the
       producer's in-memory status backup is written to the Variable. When ``True`` (default) it is
       written eagerly after every dbt node, surviving even a hard ``SIGKILL``/OOM kill. When ``False``
       it is written once, when the producer is retried, via the producer's ``on_retry_callback``: this removes
       the per-node Variable I/O, and a *graceful* failure (e.g. a dbt error) still flushes the backup
       so consumers recover as before — only a *hard* kill, where the callback cannot run, loses the
       statuses and makes the affected consumers re-run their dbt node (results stay correct). The
       long-term reliable-and-fast replacement is tracked in
       `#2771 <https://github.com/astronomer/astronomer-cosmos/issues/2771>`_ (Airflow 3.3 Task &
       Asset Store).

Automatic retries
+++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behavior
   * - **1.11.0**
     - **Unsafe.** No safeguard; producer auto-retries would relaunch ``dbt build``, while consumer
       tasks may be running their own retries.
   * - **1.11.1**
     - **Unsafe.** Same as 1.11.0.
   * - **1.11.2**
     - **Failure.** Producer ``retries`` forced to ``0`` by Cosmos — no auto-retry possible on the
       producer.
   * - **1.11.3**
     - **Failure.** Same as 1.11.2.
   * - **1.12.0**
     - **Failure.** Same as 1.11.2.
   * - **1.12.1**
     - **Failure.** Same as 1.11.2.
   * - **1.13.0**
     - **Failure.** Same as 1.11.2.
   * - **1.13.1**
     - **Failure.** Same as 1.11.2.
   * - **1.14.0**
     - **Incorrect status.** Forced ``retries=0`` on the producer is removed
       (`#2479 <https://github.com/astronomer/astronomer-cosmos/pull/2479>`_), fixing
       `#2429 <https://github.com/astronomer/astronomer-cosmos/issues/2429>`_. Producer auto-retries
       return success without re-running ``dbt build``, but Airflow does not preserve XCom across
       retries (`#2554 <https://github.com/astronomer/astronomer-cosmos/issues/2554>`_), so failed
       dbt models can be silently marked successful.
   * - **1.14.1**
     - **Works.** Producer auto-retries raise ``AirflowSkipException``; XCom is restored from the
       Variable backup so consumers read correct model statuses. Subject to the XCom backup known
       issues — see *Task-level retry — producer*.
   * - **1.14.2**
     - **Works.** Same as 1.14.1, with the XCom-backup known issues fixed
       (`#2629 <https://github.com/astronomer/astronomer-cosmos/pull/2629>`_,
       `#2683 <https://github.com/astronomer/astronomer-cosmos/pull/2683>`_ — see
       *Task-level retry — producer*). Additionally, downstream models whose upstream failed on
       the producer's first attempt are now re-run on Airflow retry instead of staying SKIPPED
       (`#2684 <https://github.com/astronomer/astronomer-cosmos/pull/2684>`_).
   * - **1.15.0**
     - **Works.** Same as 1.14.2 by default. With ``enable_watcher_reliable_retry=False`` a *graceful*
       producer failure still restores statuses on auto-retry (flushed by the ``on_retry_callback``);
       only a hard ``SIGKILL``/OOM kill loses them, making the affected consumers re-run their dbt node
       — correct, but with extra compute. See *Task-level retry — producer*.

Full DAG / TaskGroup clear
++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behavior
   * - **1.11.0**
     - **Unsafe.** Relaunches the entire ``dbt build`` — dangerous duplicate/concurrent run.
   * - **1.11.1**
     - **Unsafe.** Same as 1.11.0.
   * - **1.11.2**
     - **Unsafe.** Same as 1.11.0.
   * - **1.11.3**
     - **Unsafe.** Same as 1.11.0.
   * - **1.12.0**
     - **Unsafe.** Same as 1.11.0.
   * - **1.12.1**
     - **Unsafe.** Same as 1.11.0.
   * - **1.13.0**
     - **Works.** Producer returns success on retry without re-running ``dbt build``; consumers run
       using ``ExecutionMode.LOCAL``. Works correctly on a manual full clear.
   * - **1.13.1**
     - **Works.** Same as 1.13.0.
   * - **1.14.0**
     - **Incorrect status.** Same as 1.13.0, but Airflow does not preserve XCom across retries
       (`#2554 <https://github.com/astronomer/astronomer-cosmos/issues/2554>`_), so failed dbt
       models can be silently marked successful.
   * - **1.14.1**
     - **Works.** Producer raises ``AirflowSkipException`` (skipped, not successful); XCom is
       restored from the Variable backup so consumers read correct model statuses (subject to the
       XCom backup known issues — see *Task-level retry — producer*). For ``DbtTaskGroup``, a
       gateway task ``dbt_producer_watcher_done``
       (`#2597 <https://github.com/astronomer/astronomer-cosmos/pull/2597>`_) with
       ``trigger_rule="none_failed"`` is added downstream of the producer to absorb its skip state
       so it does not propagate to tasks downstream of the group
       (`#2594 <https://github.com/astronomer/astronomer-cosmos/issues/2594>`_). The gateway is
       only added for ``DbtTaskGroup`` — ``DbtDag`` does not need it.
   * - **1.14.2**
     - **Works.** Same as 1.14.1, with the XCom-backup known issues fixed
       (`#2629 <https://github.com/astronomer/astronomer-cosmos/pull/2629>`_,
       `#2683 <https://github.com/astronomer/astronomer-cosmos/pull/2683>`_ — see
       *Task-level retry — producer*).
   * - **1.15.0**
     - **Works.** Same as 1.14.2. ``enable_watcher_reliable_retry`` does not change full-clear
       behaviour; its only effect is on the producer's failure-time backup — see
       *Task-level retry — producer*.

Avoid duplicate or concurrent runs of the same dbt transformation in the same DAG run
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Version
     - Behavior
   * - **1.11.0**
     - **Not met.** Producer auto-retry, manual clear, or full DAG/TaskGroup clear relaunches
       ``dbt build`` and re-runs all transformations — potentially in parallel with consumer
       fallback runs (``ExecutionMode.LOCAL`` behavior) of the same models.
   * - **1.11.1**
     - **Not met.** Same as 1.11.0.
   * - **1.11.2**
     - **Not met.** Producer ``retries`` forced to ``0`` — auto-re-run is impossible. Manual
       producer clear or full DAG/TaskGroup clear still relaunches ``dbt build`` and may run
       concurrently with consumer fallbacks.
   * - **1.11.3**
     - **Not met.** Same as 1.11.2.
   * - **1.12.0**
     - **Not met.** Same as 1.11.2.
   * - **1.12.1**
     - **Not met.** Same as 1.11.2.
   * - **1.13.0**
     - **Not met.** Producer returns success on retry without re-running ``dbt build``, so retries
       and full clears no longer relaunch the entire build. However, when a consumer sensor times
       out and Airflow auto-retries it, the consumer's ``ExecutionMode.LOCAL`` fallback runs
       unconditionally without checking whether the producer is still running — which can cause
       concurrent runs of the same transformation. Fixed in 1.14.1 by
       `#2592 <https://github.com/astronomer/astronomer-cosmos/pull/2592>`_.
   * - **1.13.1**
     - **Not met.** Same as 1.13.0.
   * - **1.14.0**
     - **Not met.** Same as 1.13.0 — forced ``retries=0`` is lifted, but the consumer-sensor-retry
       concurrent run risk persists. Fixed in 1.14.1 by
       `#2592 <https://github.com/astronomer/astronomer-cosmos/pull/2592>`_.
   * - **1.14.1**
     - **Met.** Producer raises ``AirflowSkipException`` on retry — no ``dbt build`` re-run. On
       consumer sensor retry (`#2592 <https://github.com/astronomer/astronomer-cosmos/pull/2592>`_),
       Cosmos now checks the producer's state first: if it is still running, the sensor keeps
       polling instead of launching a duplicate ``dbt`` invocation; only after the producer reaches
       a terminal state does the consumer fall back to ``ExecutionMode.LOCAL``.
   * - **1.14.2**
     - **Met.** Same as 1.14.1. When ``depends_on_past=True`` is set on a ``DbtTaskGroup``, the
       cross-run risk where the next DAG run's producer could start before the previous run's
       consumers had completed
       (`#2596 <https://github.com/astronomer/astronomer-cosmos/issues/2596>`_) is now mitigated:
       leaf consumer tasks are wired upstream of the producer-done gateway, and the producer
       plus watcher tasks carry ``wait_for_downstream=True``
       (`#2615 <https://github.com/astronomer/astronomer-cosmos/pull/2615>`_), so the task group
       behaves as a single atomic unit across runs. Topology is unchanged for the default
       ``depends_on_past=False``.
   * - **1.15.0**
     - **Met.** Same as 1.14.2. ``enable_watcher_reliable_retry`` does not affect duplicate or
       concurrent-run protection — the producer still skips on retry, and consumer fallbacks remain
       gated on the producer's terminal state.
