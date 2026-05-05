.. _faq:

Frequently asked questions
==========================

This page collects common questions about using Astronomer Cosmos.

Can I run a combination of dbt Core and dbt Cloud in the same Airflow deployment?
---------------------------------------------------------------------------------

Yes. dbt Core (via Cosmos) and dbt Cloud (now dbt Platform) can run side by
side in the same Airflow deployment without conflict.

- **dbt Core** is orchestrated with Cosmos, typically via ``DbtDag`` or
  ``DbtTaskGroup`` instances.
- **dbt Cloud** is orchestrated by Airflow's official
  `apache-airflow-providers-dbt-cloud <https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/index.html>`_
  provider (for example, ``DbtCloudRunJobOperator`` and
  ``DbtCloudJobRunSensor``), which triggers and monitors jobs defined in
  dbt Cloud.

This gives you full flexibility:

- **Different DAGs**: Cosmos-rendered DAGs for dbt Core projects and plain
  Airflow DAGs using dbt Cloud operators for dbt Platform projects, all
  scheduled in the same Airflow deployment.
- **The same DAG**: mix and match — for example, a dbt Cloud job triggers
  first via ``DbtCloudRunJobOperator``, then a Cosmos ``DbtTaskGroup`` runs
  a downstream dbt Core project against the resulting tables (or
  vice-versa).
- **Data-aware handoffs**: a Cosmos task can emit an Airflow Dataset
  (Airflow 2) or Asset (Airflow 3) on completion that triggers a dbt Cloud
  DAG, or vice-versa. The underlying concept is the same; the name changed
  from Dataset to Asset in Airflow 3.

Are there any Airflow 3 features that pair particularly well with Cosmos and dbt?
---------------------------------------------------------------------------------

Yes — two highlights:

- **dbt docs plugin (rebuilt for Airflow 3)**: Cosmos uses Airflow 3's
  overhauled FastAPI plugin model and supports rendering docs for multiple
  dbt projects in the same Airflow UI. Requires Airflow ≥ 3.1. See
  :doc:`guides/dbt_docs/hosting-docs`.
- **Data-aware scheduling (Datasets / Assets)**: Cosmos automatically
  emits an Airflow Dataset (Airflow 2) or Asset (Airflow 3) for each dbt
  model it runs, so downstream DAGs can be triggered when the model is
  updated — no time-based polling or cross-DAG sensors required. See
  :doc:`guides/run_dbt/customization/scheduling`.

How can I reuse dbt artefacts across Cosmos tasks?
--------------------------------------------------

The recommended pattern is to build the dbt artefacts **once** at
deployment time and ship them alongside Cosmos, rather than regenerating
them on every task run.

Run the following commands ahead of time — for example, by baking the
result into your container image, or via ``astro dbt deploy`` on Astro:

.. code-block:: bash

    dbt deps
    dbt ls

These commands produce the artefacts that Cosmos can reuse:

- ``manifest.json`` — pass it to Cosmos via
  ``ProjectConfig(manifest_path=...)`` with ``LoadMode.DBT_MANIFEST`` to
  skip ``dbt ls`` at DAG parse time. See
  :doc:`guides/translate_dbt_to_airflow/parsing-methods`.
- ``partial_parse.msgpack`` — Cosmos automatically picks this up from the
  dbt project's ``target`` directory to speed up both DAG parsing and task
  execution. See :doc:`guides/run_dbt/customization/partial-parsing`.
- ``dbt_packages/`` — pre-installing dbt packages avoids running
  ``dbt deps`` on each task.

How do I decide which dbt models to group in a ``DbtDag`` or ``DbtTaskGroup``?
------------------------------------------------------------------------------

There isn't a one-size-fits-all answer — the right split depends on what
your team optimises for. A few useful axes to consider:

- **Project organisation / folder structure.** If your dbt project is
  already organised by domain (``marts/finance``, ``marts/marketing``,
  etc.), the lowest-friction option is to mirror that in Cosmos.
  ``RenderConfig(group_nodes_by_folder=True)`` automatically creates one
  TaskGroup per folder. This is a strong default when the project
  structure already reflects how the team thinks. See
  :doc:`guides/translate_dbt_to_airflow/render-config`.
- **Tags and selectors.** When the folder layout doesn't match ownership
  or scheduling needs, tag-based selection (for example,
  ``select=["tag:hourly"]`` or ``select=["tag:finance"]``) gives you finer
  control. Creating multiple Cosmos DAGs or TaskGroups, each scoped to a
  selector, lets different schedules and ownership boundaries coexist
  cleanly. See :doc:`guides/translate_dbt_to_airflow/selecting-excluding`.
- **Schedule and freshness requirements.** Models that need to run hourly
  shouldn't be tied to a daily DAG just because they live in the same
  folder. When cadence varies, splitting by schedule is often the clearest
  signal — even if it introduces some duplication in lineage.
- **Ownership and on-call.** If different teams own different parts of
  the project, aligning DAG boundaries with those ownership lines
  simplifies failure routing, retries, and SLAs. Cosmos
  :doc:`task callbacks <guides/run_dbt/callbacks/callbacks>` can then map
  directly to the owning team's alerting.
- **Criticality / SLAs.** Isolating mission-critical models into their
  own DAGs (with stricter retries, alerting, and ``tag:prod_critical``
  selectors) helps protect production reliability from noisier or
  experimental workloads.
- **Resource profile.** Grouping heavy or long-running models together
  lets you assign dedicated pools, queues, or larger Kubernetes pods (in
  ``KUBERNETES`` or ``WATCHER_KUBERNETES`` modes) without over-provisioning
  the rest of the project.
- **Cross-project dependencies.** If you're working with multiple dbt
  projects, Cosmos supports this natively. Treat each project as its own
  DAG or TaskGroup and define explicit dependencies between them, rather
  than forcing everything into a single mono-DAG. See
  :doc:`guides/multi_project/multi-project`.

How can I fetch the artefacts generated by Cosmos tasks and run custom logic on top of them?
--------------------------------------------------------------------------------------------

Use Cosmos **callbacks**. A callback is a function Cosmos runs as part of
task execution, before the dbt ``target`` folder is cleaned up — so it has
direct access to the artefacts dbt produced (``manifest.json``,
``run_results.json``, ``catalog.json``, ``sources.json``, compiled SQL,
etc.).

Common things you can do from a callback:

- Read ``run_results.json`` to extract failing nodes, timings, or row counts.
- Upload artefacts to object storage (S3, GCS, Azure WASB) — Cosmos ships
  built-in helpers for this in ``cosmos/io.py``.
- Log or archive compiled SQL for audit or debugging.
- Trigger follow-up logic such as Snowflake queries, alerts, or downstream
  notifications.

See :doc:`guides/run_dbt/callbacks/callbacks` for the full callback API,
built-in helpers, and end-to-end examples.

Can my dbt command use Airflow parameters or variables computed at run time?
----------------------------------------------------------------------------

Yes. Although Airflow does not render templated fields during DAG parsing,
Cosmos resolves them **at task execution time**. To opt in, pass the
templated values via ``operator_args``.

Templatable fields include ``vars``, ``freshness``, and — when passed via
``DbtDag(operator_args=...)`` (or directly to a standalone operator
instance) — ``select``, ``selector``, and ``exclude``.

Example: passing Airflow date-aware Jinja templates as dbt ``vars`` via
``DbtDag`` and ``operator_args``:

.. code-block:: python

    from datetime import datetime

    from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig

    jaffle_shop_dated = DbtDag(
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt/jaffle_shop"),
        profile_config=ProfileConfig(...),
        execution_config=ExecutionConfig(...),
        operator_args={
            "vars": {
                "run_start_date": "{{ data_interval_start | ds }}",
                "run_end_date": "{{ data_interval_end | ds }}",
            },
        },
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        dag_id="jaffle_shop_dated",
    )

At task execution time, Airflow renders the templates and Cosmos forwards
the resolved values to dbt as ``--vars``, so each run uses the
corresponding execution window.

.. note::

   Templated ``vars`` and ``env`` are not used when Cosmos parses the DAG
   with ``LoadMode.DBT_LS``. If the values need to influence DAG rendering
   (for example, to drive node selection), set them on
   ``ProjectConfig.dbt_vars`` instead. See
   :doc:`guides/run_dbt/operators/operator-args` for the full list of
   template fields and caveats.

.. note::

   This FAQ is a work in progress. If your question is not answered here,
   please open an issue in the
   `GitHub repository <https://github.com/astronomer/astronomer-cosmos/issues/new/choose>`_
   or ask in the ``#airflow-dbt`` channel of the Apache Airflow® Slack.
