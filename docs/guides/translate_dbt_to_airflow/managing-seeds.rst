.. _managing-seeds:

Managing seeds
==============

.. note::
    ``SeedRenderingBehavior`` is available for cosmos >= 1.15.0.

By default, Cosmos renders every dbt seed and runs ``dbt seed`` on each DAG run. In many production pipelines,
seeds change rarely, so re-running them on every execution is unnecessary. You can control this behaviour using the
``seed_rendering_behavior`` field in the ``RenderConfig`` object. This is how it works:

- **always** (default): Cosmos renders the seed and runs ``dbt seed`` on every execution. This preserves the
  original Cosmos behaviour.

- **when_seed_changes**: Cosmos renders the seed, but only runs ``dbt seed`` when the seed's CSV content has changed
  since the last successful run. When the content is unchanged, the task succeeds without running ``dbt seed``.

- **render_only**: Cosmos renders the seed as a no-op ``EmptyOperator`` so it stays visible in the DAG topology and
  lineage, but ``dbt seed`` is never run. This is useful when seeds are managed outside of Cosmos.

- **none**: Cosmos does not render the seed in the DAG/TaskGroup at all.

Example:

.. code-block:: python

    from cosmos import DbtTaskGroup, RenderConfig
    from cosmos.constants import SeedRenderingBehavior

    jaffle_shop = DbtTaskGroup(
        render_config=RenderConfig(
            seed_rendering_behavior=SeedRenderingBehavior.WHEN_SEED_CHANGES,
        )
    )


Detecting seed changes
~~~~~~~~~~~~~~~~~~~~~~

When ``seed_rendering_behavior`` is set to ``when_seed_changes``, Cosmos computes the seed's content checksum and
compares it against the checksum recorded after the last successful run:

- The checksum is the SHA256 of the seed's CSV content, computed from the seed file when available. Computing it from the
  file (rather than reading dbt's per-node manifest checksum) keeps change detection consistent whether the project
  is loaded via ``LoadMode.DBT_MANIFEST`` or ``LoadMode.DBT_LS``.
- The last-seen checksum is persisted as an Airflow Variable, scoped per ``DbtDag``/``DbtTaskGroup`` and seed, so the
  same seed rendered in different DAGs tracks its state independently and one DAG never causes another to skip a seed.
- Passing ``full_refresh=True`` always runs the seed, bypassing change detection. This path does not update the persisted
  checksum, so the next non-full-refresh run may still execute once to record it.
- On a run where the seed is skipped because it is unchanged, the task does not emit its Airflow dataset, since no
  data was loaded.


Limitations
~~~~~~~~~~~

- ``when_seed_changes`` is only supported for ``ExecutionMode.LOCAL``, ``ExecutionMode.VIRTUALENV`` and
  ``ExecutionMode.AIRFLOW_ASYNC``, which run dbt directly on the Airflow worker with access to the seed files.
  Configuring it with any other execution mode raises a ``CosmosValueError`` at DAG-build time.
- ``when_seed_changes`` is incompatible with ``TestBehavior.BUILD`` (under ``BUILD`` seeds run via ``dbt build`` and
  cannot be selectively skipped); this combination also raises a ``CosmosValueError``.
- Under ``ExecutionMode.WATCHER``, a single ``dbt build`` runs the whole selection, so seeds run regardless of this
  setting. Only ``always`` is meaningful for the watcher execution mode; ``none`` and ``render_only`` change only what
  is rendered, not whether the underlying ``dbt build`` loads the seed.

Example:

.. literalinclude:: ../../../dev/dags/example_seed_rendering.py
    :language: python
    :start-after: [START seed_rendering_example]
    :end-before: [END seed_rendering_example]
