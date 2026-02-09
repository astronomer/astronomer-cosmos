Memory Optimization Options for Astronomer Cosmos
==================================================

This document outlines various options to reduce memory usage when using astronomer-cosmos.

1. Enable Memory-Optimized Imports (Recommended)
-------------------------------------------------

**Impact**: High - Reduces memory footprint even when Cosmos is installed but not actively used.

**Configuration**:

.. code-block:: bash

   # In airflow.cfg
   [cosmos]
   enable_memory_optimised_imports = True

   # Or via environment variable
   export AIRFLOW__COSMOS__ENABLE_MEMORY_OPTIMISED_IMPORTS=True

**What it does**: Disables eager imports in ``cosmos/__init__.py``, preventing unused modules and classes from being loaded into memory.

**Note**: When enabled, you must use full module paths for imports:

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

**Impact**: High - Avoids running ``dbt ls`` subprocess which can consume significant CPU and memory.

**Configuration**:

.. code-block:: python

   from cosmos import DbtDag, ProjectConfig, RenderConfig, LoadMode

   DbtDag(
       project_config=ProjectConfig(dbt_project_path="/path/to/dbt/project"),
       render_config=RenderConfig(
           load_method=LoadMode.DBT_MANIFEST,  # Use manifest instead of DBT_LS
       ),
       # ...
   )

**What it does**: Uses pre-compiled ``manifest.json`` file instead of running ``dbt ls`` command, avoiding subprocess overhead and memory usage.

**Requirements**: You need a ``manifest.json`` file (can be generated with ``dbt compile`` or ``dbt run``).

-------------------------------------------------------------------------------

3. Use DBT_RUNNER Invocation Mode (Default since Cosmos 1.9)
-------------------------------------------------------------

**Impact**: Medium-High - Reduces subprocess overhead and memory usage compared to subprocess mode.

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

.. code-block:: bash

   # In airflow.cfg (should be enabled, not disabled)
   [cosmos]
   enable_cache_partial_parse = True

   # Also ensure mock profiles are disabled for partial parse to work
   # In your DbtDag:
   render_config=RenderConfig(
       enable_mock_profile=False,  # Required for partial parse
   )

**What it does**: Uses dbt's ``partial_parse.msgpack`` to avoid re-parsing unchanged parts of the project, reducing memory and CPU usage.

**Default**: ``True``

-------------------------------------------------------------------------------

5. Use ExecutionMode.WATCHER
-----------------------------

**Impact**: Very High - Dramatically reduces Airflow worker slot usage and memory consumption.

**Configuration**:

.. code-block:: python

   from cosmos import DbtDag, ProjectConfig, RenderConfig, ExecutionMode, ExecutionConfig

   DbtDag(
       project_config=ProjectConfig(dbt_project_path="/path/to/dbt/project"),
       execution_config=ExecutionConfig(
           execution_mode=ExecutionMode.WATCHER,
       ),
       # ...
   )

**What it does**:

- **Producer-Consumer Pattern**: Runs a single ``dbt build`` command via one producer task, instead of one dbt command per model
- **Low Concurrency**: Consumer tasks are lightweight sensors that use deferrable execution (default since Cosmos 1.12.0), freeing up worker slots
- **Memory Efficient**: Consumer sensors require minimal memory/CPU on first try (they just poll XCom)
- **Performance**: Can be up to 5× faster than ``ExecutionMode.LOCAL`` while using far fewer worker slots

**Key Benefits**:

1. **Single Producer Task**: Only one task runs the actual dbt build, reducing memory overhead
2. **Deferrable Sensors**: Consumer sensors use deferrable execution by default, freeing worker slots while waiting
3. **Low Resource Usage**: Consumer sensors are lightweight and only consume resources when actively polling
4. **Queue Support**: Configure ``watcher_dbt_execution_queue`` to route dbt execution tasks to a queue with larger resources

**Configuration for Worker Queue**:

.. code-block:: bash

   # In airflow.cfg
   [cosmos]
   watcher_dbt_execution_queue = high_memory_queue

   # Or via environment variable
   export AIRFLOW__COSMOS__WATCHER_DBT_EXECUTION_QUEUE=high_memory_queue

**How it works**:

- **Producer Tasks**: The ``DbtProducerWatcherOperator`` (producer task) uses the configured queue on its first execution, as it runs the full dbt build command and requires significant resources (e.g., ~700MB for a project with ~200 models)
- **Consumer Sensor Tasks**: On their first attempt, consumer sensors are lightweight (~200MB) and run on their default queue. On retry attempts (when they execute dbt commands), they are automatically assigned to the configured ``watcher_dbt_execution_queue`` if set
- **Resource Optimization**: This allows you to use lightweight workers for initial sensor execution and high-resource workers for dbt command execution, optimizing resource allocation
- **Automatic Assignment**: Cosmos uses Airflow's cluster policy feature (``task_instance_mutation_hook``) to automatically assign tasks to the specified queue at runtime

**Thread Configuration**: Control dbt concurrency via ``threads`` in your dbt profile:

.. code-block:: python

   from cosmos.config import ProfileConfig
   from cosmos.profiles import PostgresUserPasswordProfileMapping

   profile_config = ProfileConfig(
       profile_name="jaffle_shop",
       target_name="prod",
       profile_mapping=PostgresUserPasswordProfileMapping(
           conn_id="postgres_connection",
           profile_args={"threads": 8},  # Adjust based on worker capacity
       ),
   )

**Requirements**:

- Cosmos 1.11.0+ (deferrable execution requires Cosmos 1.12.0+)
- dbt installed alongside Airflow (for producer task)
- Triggerer component enabled in Airflow (for deferrable sensors)

**Performance Impact**:

- Up to 80% reduction in total DAG runtime
- Dramatically fewer worker slots used (1 producer + lightweight sensors vs. 1 task per model)
- Lower memory usage per DAG run

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
