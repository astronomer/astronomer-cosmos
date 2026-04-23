:orphan:

.. _watcher-retry-history:

Watcher Retry Behaviour History
-------------------------------

This page documents the evolution of retry behaviour in ``ExecutionMode.WATCHER`` across Cosmos releases.

Goals
+++++

* Users should be able to retry individual tasks via Airflow retry
* Users should be able to retry the whole DAG via Airflow clear
* Users should be able to retry the whole DAG via Airflow automatic retry — so humans do not need to intervene when the DAG fails
* The Airflow DAG should be marked as successful if the dbt pipeline runs successfully

History
+++++++

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Version
     - Is the DAG successful when dbt succeeds?
     - Retry behaviour
   * - **1.11.0**
     - Yes on a clean first run. Retrying the whole DAG may cause concurrent dbt executions.
     - **Task-level (consumer):** LOCAL fallback (``dbt run --select <model>``).
       **Task-level (producer) or full DAG/TaskGroup clear:** relaunches the entire ``dbt build`` —
       dangerous duplicate/concurrent run.
       **Automatic retries:** no safeguard yet; producer auto-retries would also relaunch ``dbt build``.
   * - **1.11.2**
     - Same as 1.11.0.
     - **Automatic retries:** producer ``retries`` now forced to ``0`` by Cosmos — no auto-retry
       possible on the producer.
   * - **1.12.0**
     - Same as 1.11.0.
     - Same constraints as 1.11.2. Compiled SQL now renders correctly in the LOCAL fallback retry
       path (`#2209 <https://github.com/astronomer/astronomer-cosmos/pull/2209>`_). Deferrable sensors
       (`#2127 <https://github.com/astronomer/astronomer-cosmos/pull/2127>`_) mean consumers detect
       producer failure faster, freeing worker slots sooner.
   * - **1.13.0**
     - **No** — even if all consumer tasks retry and succeed via LOCAL fallback, the producer task
       remains in a **failed** state (retries forced to ``0``). Manual clear is always required to
       clear the producer task to get the DAG to succeed.
     - **Main change:** producer now returns success on ``try_number > 1``
       (`#2283 <https://github.com/astronomer/astronomer-cosmos/pull/2283>`_) — logs an informational
       message, does not re-run ``dbt build``. This also fixed a side effect where empty/ephemeral
       models would hang consumers (`#2279 <https://github.com/astronomer/astronomer-cosmos/pull/2279>`_).
       However, producer ``retries`` is still forced to ``0``, so the success-on-retry path is only
       reached via manual clear.
       **Full DAG/TaskGroup clear:** producer returns success on retry; consumers do LOCAL fallback.
       But producer auto-retry is impossible (forced ``retries=0``), so a full clear still requires
       manual action on the producer.
       **Automatic retries:** not possible on the producer.
   * - **1.14.0**
     - **Misleadingly yes** — the DAG appears successful, but consumer tasks that failed in the
       producer are incorrectly marked as successful.
     - **Main change:** removes the forced ``retries=0`` on the producer
       (`#2479 <https://github.com/astronomer/astronomer-cosmos/pull/2479>`_), fixing
       `#2429 <https://github.com/astronomer/astronomer-cosmos/issues/2429>`_.
       Producer auto-retries return success (no duplicate ``dbt build``).
       **Bug introduced** (`#2430 <https://github.com/astronomer/astronomer-cosmos/issues/2430>`_):
       because the producer returns success on retry, and Airflow clears XCom entries between retries,
       consumers lose the model statuses from the first attempt. They see the producer as "successful"
       and assume their models succeeded — even models that actually failed. This means failed dbt
       models are silently marked as successful in Airflow.
       **Task-level (consumer):** LOCAL fallback.
   * - **1.14.1**
     - Yes for both ``DbtDag`` and ``DbtTaskGroup``.
     - **Producer now raises** ``AirflowSkipException`` **on retry**
       (`#2559 <https://github.com/astronomer/astronomer-cosmos/pull/2559>`_) instead of returning
       success — this makes the producer state explicitly "skipped" rather than misleadingly
       "successful". XCom state is backed up to an Airflow Variable during execution and restored
       on retry so consumers correctly read model statuses. On success, the backup Variable is
       automatically cleaned up.
       **DbtTaskGroup fix** (`#2597 <https://github.com/astronomer/astronomer-cosmos/pull/2597>`_):
       since the producer is now skipped (not successful), a gateway task
       (``dbt_producer_watcher_done``) is added inside the ``DbtTaskGroup``, downstream of the
       producer with ``trigger_rule="none_failed"``. This absorbs the producer's skip state so it
       does not propagate to tasks downstream of the group
       (`#2594 <https://github.com/astronomer/astronomer-cosmos/issues/2594>`_). The gateway is only
       added for ``DbtTaskGroup`` — ``DbtDag`` does not need it.
       **Task-level (consumer):** LOCAL fallback.
