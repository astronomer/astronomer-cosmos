# Feature Request: Add Databricks Support for ExecutionMode.AIRFLOW_ASYNC

## Summary
Implement `DbtRunAirflowAsyncDatabricksOperator` to enable asynchronous execution for Databricks, similar to the existing BigQuery async implementation. This would allow Databricks users to benefit from ~35% performance improvements via Airflow deferrable operators.

## Current State
- **Documentation states**: "This execution mode can reduce the runtime by 35% in comparison to Cosmos LOCAL execution mode, but is **currently only available for BigQuery**" ([docs link](https://astronomer.github.io/astronomer-cosmos/getting_started/async-execution-model.html))
- **Code status**: `cosmos/operators/_asynchronous/databricks.py` is a skeleton with "TODO: Implement it" comment
- **Test exists**: `tests/operators/_asynchronous/test_databricks.py` only tests `NotImplementedError()`
- **Supported databases**: `_SUPPORTED_DATABASES = [BIGQUERY_PROFILE_TYPE]` in `airflow_async.py`

## Why This Matters
### Performance Impact
The BigQuery async implementation shows:
- **35% faster** execution compared to LOCAL mode
- **Very Fast** task duration (per execution modes comparison table)
- **Medium** environment isolation while maintaining performance

### User Need
Databricks is a major data platform with many Cosmos users who would benefit from:
1. Improved task throughput via Airflow Trigger framework
2. Better resource utilization (workers freed while SQL runs)
3. Faster task execution through pre-compiled SQL

## Proposed Implementation
Follow the existing BigQuery pattern in `cosmos/operators/_asynchronous/bigquery.py`:

### 1. Create Mock Adapter Function
```python
def _mock_databricks_adapter() -> None:
    """Mock Databricks adapter to avoid unnecessary dbt overhead during async execution"""
    # Similar to _mock_bigquery_adapter() but for Databricks
    # Handles DatabricksConnectionManager.execute()
```

### 2. Implement DbtRunAirflowAsyncDatabricksOperator
Inherit from:
- `DatabricksSqlOperator` (from `apache-airflow-providers-databricks`)
- `AbstractDbtLocalBase`

Key methods:
- `__init__`: Setup connection, configuration, dbt kwargs
- `base_cmd`: Return `["run"]`
- `get_sql_from_xcom()`: Fetch compiled SQL from XCom
- `get_remote_sql()`: Fetch from remote object storage
- `execute()`: Run SQL via Databricks operator (deferrable)
- `_register_event()`: Emit Airflow datasets/assets

### 3. Update Supporting Files
- **`cosmos/operators/_asynchronous/__init__.py`**: Add Databricks mock function support
- **`cosmos/operators/airflow_async.py`**: Add `"databricks"` to `_SUPPORTED_DATABASES`
- **`cosmos/constants.py`**: Define `DATABRICKS_PROFILE_TYPE` if not exists
- **Documentation**: Update `async-execution-mode.rst` to list Databricks as supported

### 4. Testing
- Unit tests for operator initialization
- SQL fetch tests (XCom + remote)
- Mock adapter tests
- Integration test with deferrable execution
- Dataset/asset emission tests

## Questions for Maintainers
Before implementation, I'd like guidance on:

1. **Databricks Operator Choice**: Should we use:
   - `DatabricksSqlOperator` (for SQL execution)
   - `DatabricksSubmitRunOperator` (for job submissions)
   - `DatabricksRunNowOperator` (for existing jobs)

2. **Connection Handling**: Databricks has multiple auth methods (token, OAuth, etc.). Should we:
   - Support all auth methods from day 1?
   - Start with token-based auth (most common)?

3. **SQL Execution Method**: How should we execute the compiled SQL?
   - Direct SQL statement execution?
   - Use Databricks SQL warehouse endpoints?

4. **Testing Requirements**: Do you prefer:
   - Mock-based tests only (faster CI)?
   - Integration tests with Databricks Community Edition?
   - Both?

5. **Profile Mapping**: Are there specific Databricks profile mappings we should prioritize?

## Benefits
Once implemented:
- ✅ Databricks users get 35% performance improvement
- ✅ Consistency with BigQuery async implementation
- ✅ Unlocks async execution for major cloud platform
- ✅ Maintains Cosmos patterns and architecture

## I'd Like to Contribute!
I'm willing to implement this feature following your guidance. Having learned from my previous PR attempt, I wanted to:
1. **Validate the need first** (documented limitation confirmed ✅)
2. **Get maintainer input** before coding (asking for guidance here)
3. **Follow existing patterns** (using BigQuery as reference)
4. **Ensure proper testing** (comprehensive test coverage)

Would the maintainers be interested in this contribution? If yes, I'll proceed with implementation based on your answers to the questions above.

## Related Files
- Current skeleton: `cosmos/operators/_asynchronous/databricks.py`
- BigQuery reference: `cosmos/operators/_asynchronous/bigquery.py` (260 lines)
- Setup operator: `cosmos/operators/_asynchronous/__init__.py`
- Documentation: `docs/getting_started/async-execution-mode.rst`
