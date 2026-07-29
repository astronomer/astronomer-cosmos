:orphan:

.. _watcher-benchmark:

Watcher benchmark
=================

This page captures the detailed cluster setup, per-configuration tables, and analysis behind the ``ExecutionMode.LOCAL`` versus ``ExecutionMode.WATCHER`` benchmark dated 2026-05-15. For a concise summary, see the "Performance gains" section in :ref:`watcher-execution-mode`.

We ran the comparison with the `astronomer/cosmos-benchmark <https://github.com/astronomer/cosmos-benchmark>`_ project on the official `Apache Airflow Helm chart <https://airflow.apache.org/docs/helm-chart/stable/index.html>`_, with a dedicated worker pool for the heavy producer ``dbt build`` step and a separate pool for the per-model sensor tasks.

The full setup script, raw per-rep data, and a reproducible sweep recipe are published in the `cosmos-benchmark readme <https://github.com/astronomer/cosmos-benchmark/blob/main/benchmark/readme.md#results-local-vs-watcher-2026-05-15>`_.

Cluster setup
~~~~~~~~~~~~~

- Apache Airflow Helm chart ``1.21.0`` (Airflow ``3.2.0``)
- ``astronomer-cosmos==1.14.1``, ``dbt-bigquery==1.9``
- dbt project: ``google/fhir-dbt-analytics`` (2 seeds, 52 sources, 185 models)
- Producer pool: 1 replica, ``cpu=1`` / ``memory=2Gi``
- Consumer pool: 9 replicas, ``cpu=1`` / ``memory=2Gi`` with ``worker_concurrency=2`` (18 task slots total)
- Airflow ``parallelism=16``
- Cosmos ``watcher_dbt_producer_queue=producer`` routing so the producer ``dbt build`` always lands on the dedicated pool
- 5 repetitions per configuration; wall time below is ``mean ± sample-stdev (n=5)``

Timing and CPU
~~~~~~~~~~~~~~

+---------+---------+-----------+----------------+-----------+-----------------+
| Mode    | Threads | Wall time | Producer       | Producer  | Total consumers |
|         |         | (minutes) | time (minutes) | max CPU   | max CPU         |
+=========+=========+===========+================+===========+=================+
| LOCAL   | N/A     | 8.9 ± 0.2 | N/A            | N/A       | 4.30            |
+---------+---------+-----------+----------------+-----------+-----------------+
| WATCHER | 4       | 7.5 ± 0.2 | 7.3            | 0.28      | 7.96            |
+---------+---------+-----------+----------------+-----------+-----------------+
| WATCHER | 8       | 5.2 ± 0.2 | 4.2            | 0.54      | 8.05            |
+---------+---------+-----------+----------------+-----------+-----------------+
| WATCHER | 12      | 5.6 ± 1.2 | 4.3            | 0.70      | 8.15            |
+---------+---------+-----------+----------------+-----------+-----------------+
| WATCHER | 16      | 5.2 ± 0.3 | 3.1            | 0.83      | 8.30            |
+---------+---------+-----------+----------------+-----------+-----------------+

``Producer max CPU`` is peak cores observed for the single producer pod (capacity: 1 core). ``Total consumers max CPU`` is peak cores summed across the 9 consumer pods (combined capacity: 9 cores); each individual consumer pod is bounded by its 1-core limit.

Peak memory by pool
~~~~~~~~~~~~~~~~~~~

+---------+---------+---------------+-----------------+
| Mode    | Threads | Producer peak | Total consumers |
|         |         | memory (GiB)  | peak memory     |
|         |         |               | (GiB)           |
+=========+=========+===============+=================+
| LOCAL   | N/A     | N/A           | 10.0            |
+---------+---------+---------------+-----------------+
| WATCHER | 4       | 0.8           | 8.1             |
+---------+---------+---------------+-----------------+
| WATCHER | 8       | 0.8           | 8.5             |
+---------+---------+---------------+-----------------+
| WATCHER | 12      | 0.8           | 8.7             |
+---------+---------+---------------+-----------------+
| WATCHER | 16      | 0.8           | 8.6             |
+---------+---------+---------------+-----------------+

``Producer peak memory`` is for the single producer pod (capacity: 2 GiB). ``Total consumers peak memory`` is summed across the 9 consumer pods (combined capacity: 18 GiB); each individual consumer pod is bounded by its 2 GiB limit.

Analysis
~~~~~~~~

- ``WATCHER`` beat ``LOCAL`` at every thread count we tested. Even at dbt's default of 4 threads, ``WATCHER`` cut wall-clock runtime by about 15% (7.5 vs 8.9 minutes). With 8 threads or more, ``WATCHER`` ran the DAG roughly 41% faster than ``LOCAL``.
- ``threads=8`` is a strong default. Past 8 threads the producer ``dbt build`` itself kept getting faster (4.2 minutes at ``threads=8`` versus 3.1 minutes at ``threads=16``), but the consumer sensor tasks took correspondingly longer to wake up and finalise, so total wall time plateaued (we are tracking the investigation of this consumer-side behaviour in `astronomer/astronomer-cosmos#2657 <https://github.com/astronomer/astronomer-cosmos/issues/2657>`_ and will update the findings as they become available). Start at 8 and only push higher if your producer task is the bottleneck.
- Producer CPU rises with ``threads``, but sub-linearly. Going from 4 to 16 threads pushed producer peak CPU from 0.28 to 0.83 cores. dbt threads spend most of their time waiting on the warehouse, so most extra threads do not consume an extra core. Sizing the producer pod for roughly 1 core covers ``threads=16`` comfortably.
- ``LOCAL`` is bound by your data warehouse, not Airflow. Under ``LOCAL`` the consumer pool peaked at 4.30 of 9 available cores (about 48%), because each dbt task spent most of its time waiting on BigQuery. Adding more Airflow workers will not move that ceiling. ``WATCHER`` instead saturates the consumer pool to roughly 8 cores via lightweight sensor work, and runs the heavy ``dbt build`` on the dedicated producer pod.
- ``WATCHER`` cuts consumer memory pressure. Total consumers peak memory drops from 10.0 GiB under ``LOCAL`` to roughly 8.5 GiB under ``WATCHER``, because the heavy ``dbt build`` runs in the 0.8 GiB producer pod rather than across the consumer pool's 18 concurrent dbt processes.

These numbers reflect one specific dbt project, warehouse, and worker shape. Treat them as a directional baseline and re-run the benchmark against your own pipeline before settling on a thread count.
