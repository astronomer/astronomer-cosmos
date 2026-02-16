.. _memory-optimization:

Memory Optimization Options for Astronomer Cosmos
==================================================

When running dbt pipelines with Astronomer Cosmos, the framework executes dbt commands that can consume significant memory resources. In high-memory scenarios, tasks may reach a zombie state or workers may be killed due to Out of Memory (OOM) errors, leading to pipeline failures and reduced reliability.

Cosmos provides various configuration options and execution modes to optimize memory usage, reduce worker resource consumption, and prevent OOM issues. This document outlines these memory optimization strategies, from simple configuration changes to advanced execution modes that can dramatically reduce memory footprint while maintaining or improving pipeline performance.

1. Enable Memory-Optimized Imports
-------------------------------------

**Impact**: High - Reduces memory footprint both at the DAG Processor and at Worker nodes.

**Configuration**:

.. code-block:: cfg

   # In airflow.cfg
   [cosmos]
   enable_memory_optimised_imports = True

.. code-block:: bash
   # Or via environment variable
   export AIRFLOW__COSMOS__ENABLE_MEMORY_OPTIMISED_IMPORTS=True

**What it does**: Disables eager imports in ``cosmos/__init__.py``, preventing unused modules and classes from being loaded into memory.

**Note**: When enabled, you must use full module paths for importing classes, functions and objects from Cosmos:

.. code-block:: python

   # Instead of:
   from cosmos import DbtDag, ProjectConfig, RenderConfig

   # Use:
   from cosmos.airflow.dag import DbtDag
   from cosmos.config import ProjectConfig, RenderConfig

**Default**: ``False`` (will become default in Cosmos 2.0.0)

-----------------------------------------------------------------

2. Use DBT_MANIFEST Load Mode
------------------------------

**Impact**: High - Avoids running ``dbt ls`` subprocess which can consume significant CPU and memory. This reduces memory consumption when a cache miss occurs in the DBT LS method. It may not significantly reduce the memory footprint if there is a cache hit.

**Configuration**:

.. code-block:: python

   from cosmos.airflow.dag import DbtDag
   from cosmos.config import ProjectConfig, RenderConfig, LoadMode

   DbtDag(
       project_config=ProjectConfig(dbt_project_path="/path/to/dbt/project"),
       render_config=RenderConfig(
           load_method=LoadMode.DBT_MANIFEST,  # Use manifest instead of DBT_LS
       ),
       # ...
   )

**What it does**: Uses pre-compiled ``manifest.json`` file instead of running ``dbt ls`` command, avoiding subprocess overhead and memory usage.

**Requirements**: You need a ``manifest.json`` file (can be generated with ``dbt compile`` or ``dbt run``).

---------------------------------

3. Use DBT_RUNNER Invocation Mode
-----------------------------------

* (default for ``ExecutionMode.LOCAL`` since 1.4.0, default for ``RenderConfig.DBT_LS`` since Cosmos 1.9.0)

**Impact**: Medium-High. Depends on the execution and load modes used. Can reduce subprocess overhead and memory usage compared to subprocess mode.

**Configuration**:

.. code-block:: python

   from cosmos import DbtDag, ProjectConfig, RenderConfig, LoadMode, InvocationMode

   DbtDag(
       project_config=ProjectConfig(dbt_project_path="/path/to/dbt/project"),
       render_config=RenderConfig(
           load_method=LoadMode.DBT_LS,
           invocation_mode=InvocationMode.DBT_RUNNER,  # Default since Cosmos 1.9
       ),
       # ...
   )

**What it does**: Uses ``dbtRunner`` (dbt programmatic API) instead of Python subprocess, reducing memory and CPU overhead.

**Requirements**: dbt version 1.5.0+ and dbt installed in the same Python environment as Airflow.

**Default**: ``InvocationMode.DBT_RUNNER`` (since Cosmos 1.9)

-------------------------------------------------------------------------------

4. Use Partial Parse (Keep Enabled)
------------------------------------

**Impact**: Low - Actually reduces memory by avoiding full project parsing.

**Configuration**:

.. code-block:: cfg

   # In airflow.cfg (should be enabled, not disabled)
   [cosmos]
   enable_cache_partial_parse = True

.. code-block:: python

   # Also ensure mock profiles are disabled for partial parse to work
   # In your DbtDag:
   render_config = RenderConfig(
       enable_mock_profile=False,  # Required for partial parse
   )

**What it does**: Uses dbt's ``partial_parse.msgpack`` to avoid re-parsing unchanged parts of the project, reducing memory and CPU usage.

**Default**: ``True`` since Cosmos 1.4.0

-------------------------------------------------------------------------------

5. Use ExecutionMode.WATCHER
-----------------------------

**Impact**: Very High - Dramatically reduces Airflow worker slot usage and memory consumption.

**Configuration**

- `Getting Started with ExecutionMode.WATCHER <https://astronomer.github.io/astronomer-cosmos/getting_started/watcher-execution-mode.html>`_
- `Configure a Custom Queue for Producer and Watcher Tasks in ExecutionMode.WATCHER <https://astronomer.github.io/astronomer-cosmos/getting_started/watcher-execution-mode.html#watcher-dbt-execution-queue>`_

-------------------------------------------------------------------------------

6. Control DAG-Level Concurrency with ``concurrency`` Parameter
----------------------------------------------------------------

**Impact**: High - Limits concurrent task execution per DAG based on available resources.

**Configuration**:

.. code-block:: python

   from cosmos import DbtDag, ProjectConfig, RenderConfig, ExecutionConfig, ExecutionMode

   DbtDag(
       project_config=ProjectConfig(dbt_project_path="/path/to/dbt/project"),
       execution_config=ExecutionConfig(
           execution_mode=ExecutionMode.LOCAL,  # Or WATCHER
       ),
       # DAG-level concurrency control
       concurrency=10,  # Maximum concurrent tasks across all active DAG runs
       max_active_runs=3,  # Maximum concurrent DAG runs (optional)
       # ...
   )

**What it does**:

- **``concurrency``**: The maximum number of task instances allowed to run concurrently across all active DAG runs for a given DAG
- Allows different DAGs to have different concurrency limits (e.g., one DAG runs 32 tasks at once, another runs 16)
- If not defined, defaults to the environment-level setting ``max_active_tasks_per_dag`` (default: 16)
- Works in combination with ``max_active_runs`` to control both task and DAG run concurrency

**Example: Different Concurrency for Different DAGs**:

.. code-block:: python

   # High-resource DAG - allow more concurrent tasks
   high_resource_dag = DbtDag(
       dag_id="high_resource_dbt_dag",
       concurrency=32,  # Allow 32 concurrent tasks
       max_active_runs=2,
       # ...
   )

   # Low-resource DAG - limit concurrent tasks
   low_resource_dag = DbtDag(
       dag_id="low_resource_dbt_dag",
       concurrency=8,  # Only 8 concurrent tasks
       max_active_runs=1,
       # ...
   )

**Benefits**:

- **Per-DAG Control**: Set different concurrency limits for different DAGs based on their resource needs
- **Resource Protection**: Prevent resource-intensive DAGs from overwhelming workers
- **Flexible Configuration**: Adjust concurrency without changing environment-level settings
- **Works with Pools**: Can be combined with task pools for even more granular control

**Best Practices**:

1. Set ``concurrency`` lower than your total worker capacity to leave room for other DAGs
2. Use lower ``concurrency`` values for resource-intensive DAGs (e.g., large dbt models)
3. Combine with ``max_active_runs`` to control both task and DAG run parallelism
4. Monitor task queuing - if tasks are queued for long periods, consider increasing ``concurrency``

**Reference**: `Airflow Scaling Workers Documentation <https://www.astronomer.io/docs/learn/airflow-scaling-workers>`_

-------------------------------------------------------------------------------

7. Enable Task Profiling with Debug Mode
-----------------------------------------

**Impact**: Low - Provides visibility into memory usage patterns to help identify optimization opportunities and prevent OOM issues.

**Configuration**:

.. code-block:: bash

   # In airflow.cfg
   [cosmos]
   enable_debug_mode = True

   # Or via environment variable
   export AIRFLOW__COSMOS__ENABLE_DEBUG_MODE=True

**What it does**: When enabled, Cosmos tracks memory utilization for its tasks during execution and pushes the peak memory usage (in MB) to XCom under the key ``cosmos_debug_max_memory_mb``. This enables you to:

- **Profile Memory Usage**: Identify which tasks consume the most memory
- **Optimize Resource Allocation**: Set appropriate memory limits and worker queue assignments based on actual usage
- **Track Memory Trends**: Monitor memory usage over time to detect regressions or improvements

**How to Access Memory Data**:

The peak memory usage is stored in XCom and can be accessed via the Airflow UI

**Requirements**:

- ``psutil`` package must be installed in your Airflow environment
- Debug mode adds minimal overhead (memory polling occurs at configurable intervals)

**Configuration for Poll Interval**:

You can adjust the memory polling frequency to balance accuracy and overhead:

.. code-block:: bash

   # In airflow.cfg
   [cosmos]
   enable_debug_mode = True
   debug_memory_poll_interval_seconds = 0.5  # Default: 0.5 seconds

   # Or via environment variable
   export AIRFLOW__COSMOS__DEBUG_MEMORY_POLL_INTERVAL_SECONDS=0.5

Lower values provide more accurate peak memory measurements but may add slight overhead. Higher values reduce overhead but may miss short memory spikes.

**Default**: ``False``
