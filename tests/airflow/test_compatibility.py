import pytest
from packaging.version import Version

from cosmos.airflow.compatibility import AirflowSkipException
from cosmos.constants import AIRFLOW_VERSION


def test_airflow_skip_exception_is_a_skip_exception():
    from airflow.exceptions import AirflowException

    assert issubclass(AirflowSkipException, AirflowException)


@pytest.mark.skipif(
    AIRFLOW_VERSION < Version("3.2"),
    reason="airflow.sdk.exceptions.AirflowSkipException only exists on Airflow 3.2+",
)
def test_airflow_skip_exception_resolves_to_task_sdk_on_3_2_plus():
    """On Airflow 3.2+ the shim must resolve to the Task SDK, not the deprecated airflow.exceptions path."""
    assert AirflowSkipException.__module__ == "airflow.sdk.exceptions"


@pytest.mark.skipif(
    AIRFLOW_VERSION >= Version("3.2"),
    reason="airflow.sdk.exceptions.AirflowSkipException is absent before Airflow 3.2",
)
def test_airflow_skip_exception_falls_back_before_3_2():
    """Before Airflow 3.2 the legacy import is not deprecated, so the shim falls back to it."""
    assert AirflowSkipException.__module__ == "airflow.exceptions"
