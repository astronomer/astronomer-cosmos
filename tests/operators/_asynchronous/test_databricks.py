import pytest

from cosmos.operators._asynchronous.databricks import DbtRunAirflowAsyncDatabricksOperator


def test_execute_should_raise_not_implemented_error():
    operator = DbtRunAirflowAsyncDatabricksOperator(task_id="test_task")
    with pytest.raises(NotImplementedError):
        operator.execute(context={})
