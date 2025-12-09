# Telemetry Instrumentation Guide

This document describes how to instrument Cosmos code to collect telemetry metrics.

## Overview

The telemetry system collects metrics about:
- Operator usage and types
- dbt commands executed
- Execution modes
- Database/profile types
- Task success/failure
- DAG parsing performance
- And more...

## Metrics Collected

### Task-Level Metrics

1. **Operator Type** - `cosmos.task.operator.<operator_name>` (counter)
2. **Subclass Detection** - `cosmos.task.operator.is_subclass` (counter)
3. **dbt Command** - `cosmos.task.operator_command.<dbt_command>` (counter)
4. **Execution Mode** - `cosmos.task.execution_mode.<mode>` (counter)
5. **Database/Profile Type** - `cosmos.task.database.<profile_type>` (counter)
6. **Profile Method** - `cosmos.task.profile.profile_mapping` or `cosmos.task.profile.yaml_file` (counter)
7. **Profile Mapping Class** - `cosmos.task.profile.<profile_mapping_class>` (counter)
8. **Task Origin** - `cosmos.task.origin.<task|dbt_dag|task_group>` (counter)
9. **Task Status** - `cosmos.task.status.<success|failure>` (counter)
10. **Local Invocation Mode** - `cosmos.task.execution_mode.local.invocation_mode.<subprocess|dbt_runner>` (counter)
11. **Task Duration** - `cosmos.task.operator.<operator_name>` (timer)

### DAG Parsing Metrics

12. **Load Mode** - `cosmos.dbt_parsing.load_mode.<strategy>` (counter)
13. **dbt_ls Invocation Mode** - `cosmos.task.load_mode.dbt_ls.invocation_mode.<subprocess|dbt_runner>` (counter)
14. **Parsing Duration** - `cosmos.dbt_parsing.load_mode.<strategy>.duration` (timer)
15. **Project Size** - `cosmos.dbt_parsing.load_mode.<strategy>.dbt_project_size` (gauge)

## Instrumentation Points

### 1. Task Execution (AbstractDbtBase.execute)

**Location**: `cosmos/operators/base.py`

```python
def execute(self, context: Context, **kwargs) -> Any | None:
    from cosmos.telemetry_v2 import collect_task_metrics, emit_task_status_metric
    import time

    start_time = time.time()

    # Collect task metrics at start
    collect_task_metrics(self, dag=context.get('dag'), task_group=context.get('task_group'))

    try:
        if self.extra_context:
            context_merge(context, self.extra_context)

        result = self.build_and_run_cmd(context=context, cmd_flags=self.add_cmd_flags(), **kwargs)

        # Emit success metric
        emit_task_status_metric(success=True)

        return result
    except Exception as e:
        # Emit failure metric
        emit_task_status_metric(success=False)
        raise
    finally:
        # Emit duration metric
        duration_ms = (time.time() - start_time) * 1000
        from cosmos.telemetry_v2 import emit_task_duration_metric
        emit_task_duration_metric(self, duration_ms)
```

### 2. DAG Parsing (DbtToAirflowConverter)

**Location**: `cosmos/converter.py` or wherever DAG parsing happens

```python
from cosmos.telemetry_v2 import (
    emit_load_mode_metric,
    emit_parsing_duration_metric,
    emit_project_size_metric,
    emit_dbt_ls_invocation_mode_metric,
)
import time

def render_dbt_project(...):
    start_time = time.time()
    load_mode = execution_config.load_mode.value  # or however load_mode is determined

    # Emit load mode metric
    emit_load_mode_metric(load_mode)

    try:
        # Parse dbt project
        nodes = load_dbt_project(...)

        # Emit project size
        model_count = len([n for n in nodes.values() if n.resource_type == DbtResourceType.MODEL])
        emit_project_size_metric(load_mode, model_count)

        # If using dbt_ls, emit invocation mode
        if load_mode == "dbt_ls":
            invocation_mode = execution_config.invocation_mode.value if hasattr(execution_config, 'invocation_mode') else "subprocess"
            emit_dbt_ls_invocation_mode_metric(load_mode, invocation_mode)

        return nodes
    finally:
        # Emit parsing duration
        duration_ms = (time.time() - start_time) * 1000
        emit_parsing_duration_metric(load_mode, duration_ms)
```

### 3. Task Instance Callbacks (Optional)

**Location**: Can be added via Airflow callbacks or task listeners

For task success/failure, you can also use Airflow's task callbacks:

```python
def on_success_callback(context):
    from cosmos.telemetry_v2 import emit_task_status_metric
    emit_task_status_metric(success=True)

def on_failure_callback(context):
    from cosmos.telemetry_v2 import emit_task_status_metric
    emit_task_status_metric(success=False)
```

## Usage Examples

### Simple: Collect All Task Metrics

```python
from cosmos.telemetry_v2 import collect_task_metrics

# In operator.execute() or similar
collect_task_metrics(operator=self, dag=dag, task_group=task_group)
```

### Individual Metrics

```python
from cosmos.telemetry_v2 import (
    emit_operator_metric,
    emit_dbt_command_metric,
    emit_execution_mode_metric,
)

emit_operator_metric(operator)
emit_dbt_command_metric(operator)
emit_execution_mode_metric(operator)
```

### Timing Metrics

```python
from cosmos.telemetry_v2 import emit_task_duration_metric
import time

start = time.time()
# ... do work ...
duration_ms = (time.time() - start) * 1000
emit_task_duration_metric(operator, duration_ms)
```

## Implementation Notes

1. **Performance**: Telemetry collection should be fast and non-blocking. All metrics are emitted asynchronously.

2. **Error Handling**: Telemetry failures should never break task execution. The collector functions handle exceptions internally.

3. **Conditional Collection**: Use `should_emit()` to check if telemetry is enabled before collecting metrics.

4. **Testing**: Mock the telemetry functions in tests to avoid actual metric emission.

## Next Steps

1. Add instrumentation to `AbstractDbtBase.execute()`
2. Add instrumentation to DAG parsing code
3. Add task callbacks for success/failure metrics
4. Test with real DAGs to verify metrics are collected correctly
5. Monitor metric emission in production
