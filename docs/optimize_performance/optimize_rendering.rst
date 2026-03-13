.. _optimize_rendering:

Optimize rendering
===================

Rendering your dbt project into an Airflow Dag can affect Cosmos' overall performance.

Optimize load mode
~~~~~~~~~~~~~~~~~~

The ``LoadMode`` controls how Cosmos parses the dbt project. There are multiple :ref:`load mode options <parsing-methods>`, but fundamentally, Cosmos uses two approaches:

1. Parses a user-supplied ``manifest.json`` file.
2. Parses a dbt project directory using the ``dbt ls`` command.

Because it takes additional time to parse through the dbt project, using ``dbt_ls`` can be slower than providing a pre-computed ``manifest.json`` file.

Use LoadMode.MANIFEST
++++++++++++++++++++++++

You can use ``LoadMode.MANIFEST`` as a way to improve rendering the dbt project into Dags. This method pre-computes the dbt project ``manifest.json`` as part of the CI, so that Cosmos does not need to generate the manifest.

See :ref:`dbt_manifest_load_mode` and :ref:`dbt_manifest_parsing_method` for more information about how to generate the ``manifest.json`` file and configure Cosmos to use it.

Improve LoadMode.DBT_LS performance
+++++++++++++++++++++++++++++++++++++++

If you can't use ``LoadMode.Manifest`` because you can't pre-comepute the ``manifest.json`` file as part of your CI pipeline, you can still take steps to ensure the best performance.

- Ensure that :ref:`caching_dbt_ls` is enabled and that you use :ref:`InvocationMode.DBT_RUNNER <invocation-mode>` when possible.
- Use :ref:`partial-parsing`.
- Run ``dbt deps`` as part of your CI, and disable running it in Cosmos. See :ref:`pre-install-dbt-deps`.

Reduce Dag granularity
~~~~~~~~~~~~~~~~~~~~~~~~

You can control how much of your dbt project that you want to parse into an Airflow Dag, which can allow you to speed up the rendering process by only parsing relevant sections.

Select and exclude nodes
++++++++++++++++++++++++++++++

Select only the relevant nodes for the job instead of running the entire dbt project by using selectors. See :ref:`selecting-excluding`.

Customize test behavior
++++++++++++++++++++++++++

You can select how you want Cosmos to run dbt tests.

- If tests are not required, set ``TestBehavior.NONE``.
- If tests are needed, consider ``TestBehavior.AFTER_ALL`` or ``TestBehavior.BUILD``, which are generally more efficient than the default, ``TestBehavior.AFTER_EACH``.

See :ref:`testing-behavior`.

Only run fresh dbt project sources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can configure Cosmos to skip running parts of the dbt project when the data sources are not fresh.
Rendering source nodes and specifying a custom behaviour can help skip parts of the pipeline.

- :ref:`managing-sources`
- :ref:`dag_customization`
