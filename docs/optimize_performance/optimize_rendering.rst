.. _optimize-rendering:

Optimize DAG Parsing
--------------------

Every time `Apache Airflow® <https://airflow.apache.org/>`_ parses a DAG file that contains a ``DbtDag`` or ``DbtTaskGroup``, Cosmos must load and process the
dbt project to build the corresponding Airflow task graph. The time this takes directly affects how quickly your DAGs
appear and update in Airflow. This page covers the most impactful ways to reduce that parse time.

.. tip::

   Cosmos logs the time it takes to parse each dbt project at the ``INFO`` level:

   .. code-block:: text

      Cosmos performance (<cache_id>) - [<hostname>|<pid>]: It took 0.068s to parse the dbt project for DAG using LoadMode.DBT_LS_CACHE

   Search your Airflow scheduler or DAG processor logs for ``Cosmos performance`` to measure your current parse time.


1. Choose the right LoadMode
++++++++++++++++++++++++++++

The ``LoadMode`` controls how Cosmos reads your dbt project. It is the single most impactful setting for parse-time
performance.

**Recommended: use** ``LoadMode.DBT_MANIFEST``

Parsing a pre-compiled ``manifest.json`` is the fastest option because it avoids running any dbt command at parse time.

.. code-block:: python

   from cosmos import DbtDag, ProjectConfig, RenderConfig
   from cosmos.constants import LoadMode

   DbtDag(
       dag_id="my_dbt_dag",
       project_config=ProjectConfig(
           dbt_project_path="/path/to/dbt/project",
           manifest_path="/path/to/dbt/project/target/manifest.json",
       ),
       render_config=RenderConfig(
           load_method=LoadMode.DBT_MANIFEST,
       ),
       # ...
   )

To generate the manifest, run the following from your dbt project directory (typically as part of CI/CD):

.. code-block:: bash

   dbt deps    # install packages first
   dbt compile # generates target/manifest.json

Then make the resulting ``target/manifest.json`` available to Cosmos via a local path.

For more details on all parsing methods, see :ref:`parsing-methods`.

**If you cannot pre-compute the manifest**

Use ``LoadMode.DBT_LS`` with the following optimizations to minimize parse-time overhead:

- **Enable caching** (on by default since Cosmos 1.5) so that ``dbt ls`` output is reused across parses. See :ref:`caching`.
- **Use** ``InvocationMode.DBT_RUNNER`` (default since Cosmos 1.9) to run ``dbt ls`` as a library call instead of a subprocess. See :ref:`invocation-mode`.
- **Keep partial parsing enabled** (on by default) so dbt skips re-parsing unchanged project files. See :ref:`partial-parsing`.
- **Pre-install dbt packages** in your Docker image or CI and disable runtime installation:

  .. code-block:: python

     ProjectConfig(
         dbt_project_path="/path/to/dbt/project",
         install_dbt_deps=False,  # skip dbt deps at parse time
     )

  This avoids running ``dbt deps`` on every DAG parse, which can be slow when packages need to be fetched.


2. Reduce DAG granularity
+++++++++++++++++++++++++

Fewer nodes in the Airflow DAG means faster parsing. There are two ways to reduce the number of nodes Cosmos generates.

**Select only the nodes you need**

Use ``select``, ``exclude``, or ``selector`` in ``RenderConfig`` to limit which dbt nodes are included in the DAG.
For example, to run only models tagged ``daily``:

.. code-block:: python

   RenderConfig(
       select=["tag:daily"],
   )

For the full selection syntax, see :ref:`selecting-excluding`.

**Choose an efficient TestBehavior**

The default ``TestBehavior.AFTER_EACH`` creates a separate test task after every model, which can significantly
increase the number of tasks in the DAG. Consider these alternatives:

- ``TestBehavior.NONE`` -- no test tasks are created. Use this if tests are not needed or are run separately.
- ``TestBehavior.BUILD`` -- tests run as part of the model task itself (via ``dbt build``), so no additional tasks are created.
- ``TestBehavior.AFTER_ALL`` -- a single test task runs after all models complete.

.. code-block:: python

   from cosmos.constants import TestBehavior

   RenderConfig(
       test_behavior=TestBehavior.BUILD,
   )


3. Skip stale sources
+++++++++++++++++++++

If your DAG includes multiple data sources and some may not have fresh data, you can avoid running unnecessary
branches by rendering source nodes with freshness checks. When a source is not fresh, the downstream branch can be
skipped.

To enable this, configure ``source_rendering_behavior`` in ``RenderConfig`` and customize the source node behavior
using ``node_converters``:

.. code-block:: python

   from cosmos.constants import SourceRenderingBehavior

   RenderConfig(
       source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
   )

For details on source rendering and how to customize source node behavior, see :ref:`managing-sources` and
:ref:`dag_customization`.
