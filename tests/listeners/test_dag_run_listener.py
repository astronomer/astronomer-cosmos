import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.models import DAG, DagRun
from airflow.utils.state import State
from packaging.version import Version

from cosmos import DbtRunLocalOperator, ProfileConfig, ProjectConfig
from cosmos.airflow.dag import DbtDag
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ExecutionConfig, RenderConfig
from cosmos.constants import AIRFLOW_VERSION, InvocationMode, LoadMode, SourceRenderingBehavior, TestBehavior
from cosmos.listeners.dag_run_listener import on_dag_run_failed, on_dag_run_success, total_cosmos_tasks
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_ROOT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt"
DBT_PROJECT_NAME = "jaffle_shop"

AIRFLOW_VERSION_MAJOR = AIRFLOW_VERSION.major

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


@pytest.mark.integration
def test_is_cosmos_dag_is_true():
    dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="basic_cosmos_dag",
    )
    assert total_cosmos_tasks(dag) == 13


@pytest.mark.integration
def test_total_cosmos_tasks_in_task_group():
    with DAG("test-id-dbt-compile", start_date=datetime(2022, 1, 1)) as dag:
        _ = DbtTaskGroup(
            project_config=ProjectConfig(
                DBT_ROOT_PATH / "jaffle_shop",
            ),
            profile_config=profile_config,
        )

    assert total_cosmos_tasks(dag) == 13


def test_total_cosmos_tasks_is_one():

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtRunLocalOperator(
            profile_config=profile_config,
            project_dir=DBT_ROOT_PATH / "jaffle_shop",
            task_id="run",
            install_deps=True,
            append_env=True,
        )
        run_operator

    assert total_cosmos_tasks(dag) == 1


def test_not_cosmos_dag():

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        pass

    assert total_cosmos_tasks(dag) == 0


def create_dag_run(dag: DAG, run_id: str, run_after: datetime) -> DagRun:
    if AIRFLOW_VERSION < Version("3.0"):
        # Airflow 2 and 3.0
        dag_run = dag.create_dagrun(
            state=State.NONE,
            run_id=run_id,
        )
    elif AIRFLOW_VERSION.major == 3 and AIRFLOW_VERSION.minor == 0:
        from airflow.utils.types import DagRunTriggeredByType, DagRunType

        dag_run = dag.create_dagrun(
            state=State.NONE,
            run_id=run_id,
            run_after=run_after,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.TIMETABLE,
        )
    else:
        # This is not currently working.
        # We need to find a way of testing this in Airflow 3.1 onwards
        #
        # Airflow 3.1.0+ requires DAG to be serialized to database before calling dag.create_dagrun()
        # because create_dagrun() checks for DagVersion and DagModel records
        from airflow.models.dagbag import DagBag, sync_bag_to_db
        from airflow.models.dagbundle import DagBundleModel
        from airflow.utils.session import create_session
        from airflow.utils.types import DagRunTriggeredByType, DagRunType

        # Create DagBundle if it doesn't exist (required for DagModel foreign key)
        # This mimics what get_bagged_dag does via manager.sync_bundles_to_db()
        with create_session() as session:
            dag_bundle = DagBundleModel(name="test_bundle_listener")
            session.merge(dag_bundle)
            session.commit()

        # This creates both DagModel and DagVersion records
        dagbag = DagBag(include_examples=False)
        dagbag.bag_dag(dag)
        sync_bag_to_db(dagbag, bundle_name="test_bundle_listener", bundle_version="1")

        dag_run = dag.create_dagrun(
            state=State.NONE,
            run_id=run_id,
            run_after=run_after,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.TIMETABLE,
        )
    return dag_run


@pytest.mark.skipif(
    AIRFLOW_VERSION >= Version("3.1.0"),
    reason="TODO: Fix create_dag_run to work with AF 3.1 and remove this skip.",
)
@pytest.mark.integration
@patch("cosmos.listeners.dag_run_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_dag_run_success(mock_emit_usage_metrics_if_enabled, caplog):
    caplog.set_level(logging.DEBUG)

    dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="basic_cosmos_dag",
    )
    run_id = str(uuid.uuid1())

    run_after = datetime.now(timezone.utc) - timedelta(seconds=1)
    dag_run = create_dag_run(dag, run_id, run_after)

    on_dag_run_success(dag_run, msg="test success")
    assert "Running on_dag_run_success" in caplog.text
    assert "Completed on_dag_run_success" in caplog.text
    assert mock_emit_usage_metrics_if_enabled.call_count == 1


@pytest.mark.skipif(
    AIRFLOW_VERSION >= Version("3.1.0"), reason="TODO: Fix create_dag_run to work with and remove this skip."
)
@pytest.mark.integration
@patch("cosmos.listeners.dag_run_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_dag_run_failed(mock_emit_usage_metrics_if_enabled, caplog):
    caplog.set_level(logging.DEBUG)

    dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="basic_cosmos_dag",
    )
    run_id = str(uuid.uuid1())
    run_after = datetime.now(timezone.utc) - timedelta(seconds=1)
    dag_run = create_dag_run(dag, run_id, run_after)

    on_dag_run_failed(dag_run, msg="test failed")
    assert "Running on_dag_run_failed" in caplog.text
    assert "Completed on_dag_run_failed" in caplog.text
    assert mock_emit_usage_metrics_if_enabled.call_count == 1


@pytest.mark.skipif(
    AIRFLOW_VERSION >= Version("3.1.0"),
    reason="TODO: Fix create_dag_run to work with AF 3.1 and remove this skip.",
)
@pytest.mark.integration
@patch("cosmos.listeners.dag_run_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_dag_run_success_with_telemetry_metadata(mock_emit_usage_metrics_if_enabled, caplog):
    """Test that DAG run success includes Cosmos telemetry metadata."""
    caplog.set_level(logging.DEBUG)

    dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(invocation_mode=InvocationMode.SUBPROCESS),
        render_config=RenderConfig(
            load_method=LoadMode.AUTOMATIC,
            test_behavior=TestBehavior.AFTER_EACH,
            source_rendering_behavior=SourceRenderingBehavior.NONE,
        ),
        operator_args={"install_deps": True},
        start_date=datetime(2023, 1, 1),
        dag_id="cosmos_dag_with_metadata",
    )
    run_id = str(uuid.uuid1())
    run_after = datetime.now(timezone.utc) - timedelta(seconds=1)
    dag_run = create_dag_run(dag, run_id, run_after)

    on_dag_run_success(dag_run, msg="test success")
    assert mock_emit_usage_metrics_if_enabled.call_count == 1

    # Verify telemetry call includes new metrics
    call_args = mock_emit_usage_metrics_if_enabled.call_args
    metrics = call_args[0][1]  # Second argument is the metrics dict

    # Check that Cosmos metadata fields are present
    assert "used_automatic_load_mode" in metrics
    assert "actual_load_mode" in metrics
    assert "invocation_mode" in metrics
    assert "install_deps" in metrics
    assert "uses_node_converter" in metrics
    assert "test_behavior" in metrics
    assert "source_behavior" in metrics
    assert "total_dbt_models" in metrics
    assert "selected_dbt_models" in metrics

    # Verify some expected values
    assert metrics["used_automatic_load_mode"] is True
    assert metrics["invocation_mode"] == "subprocess"
    assert metrics["install_deps"] is True
    assert metrics["uses_node_converter"] is False
    assert metrics["test_behavior"] == "after_each"
    assert metrics["source_behavior"] == "none"


@pytest.mark.skipif(
    AIRFLOW_VERSION >= Version("3.1.0"),
    reason="TODO: Fix create_dag_run to work with AF 3.1 and remove this skip.",
)
@pytest.mark.integration
@patch("cosmos.listeners.dag_run_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_dag_run_failed_with_telemetry_metadata(mock_emit_usage_metrics_if_enabled, caplog):
    """Test that DAG run failure includes Cosmos telemetry metadata."""
    caplog.set_level(logging.DEBUG)

    dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(invocation_mode=InvocationMode.DBT_RUNNER),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.NONE,
            source_rendering_behavior=SourceRenderingBehavior.ALL,
        ),
        operator_args={"install_deps": False},
        start_date=datetime(2023, 1, 1),
        dag_id="cosmos_dag_with_metadata_failed",
    )
    run_id = str(uuid.uuid1())
    run_after = datetime.now(timezone.utc) - timedelta(seconds=1)
    dag_run = create_dag_run(dag, run_id, run_after)

    on_dag_run_failed(dag_run, msg="test failed")
    assert mock_emit_usage_metrics_if_enabled.call_count == 1

    # Verify telemetry call includes new metrics
    call_args = mock_emit_usage_metrics_if_enabled.call_args
    metrics = call_args[0][1]  # Second argument is the metrics dict

    # Check that Cosmos metadata fields are present
    assert "used_automatic_load_mode" in metrics
    assert "actual_load_mode" in metrics
    assert "invocation_mode" in metrics
    assert "install_deps" in metrics
    assert "uses_node_converter" in metrics
    assert "test_behavior" in metrics
    assert "source_behavior" in metrics
    assert "total_dbt_models" in metrics
    assert "selected_dbt_models" in metrics

    # Verify some expected values for failed case
    assert metrics["used_automatic_load_mode"] is False
    assert metrics["invocation_mode"] == "dbt_runner"
    assert metrics["install_deps"] is False
    assert metrics["uses_node_converter"] is False
    assert metrics["test_behavior"] == "none"
    assert metrics["source_behavior"] == "all"
