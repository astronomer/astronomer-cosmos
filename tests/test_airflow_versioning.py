import shutil
from pathlib import Path

import pytest

from cosmos.constants import AIRFLOW_VERSION

if AIRFLOW_VERSION.major < 3:
    pytest.skip("Skipping Airflow versioning tests on Airflow 2.x", allow_module_level=True)
else:
    # Only import these when Airflow 3.x is present
    from airflow.models import DagBag, DagRun, TaskInstance
    from airflow.models.dag import DagModel
    from airflow.models.dag_version import DagVersion
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dagcode import DagCode
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.sdk import timezone
    from airflow.serialization.serialized_objects import LazyDeserializedDAG
    from airflow.utils.session import provide_session
    from airflow.utils.state import DagRunState, TaskInstanceState


DBT_PROJECT_PATH = Path(__file__).parent.parent / "dev" / "dags" / "dbt" / "jaffle_shop"


@pytest.fixture
def jaffle_shop_test_dir(tmp_path):
    """Create a test directory with the local jaffle shop project and DAG file."""
    test_path = tmp_path / "jaffle_shop"
    test_path.mkdir()
    shutil.copytree(DBT_PROJECT_PATH, test_path, dirs_exist_ok=True)

    dag_file = test_path / "basic_cosmos_dag.py"
    basic_dag_path = Path(__file__).parent.parent / "dev" / "dags" / "basic_cosmos_dag.py"
    dag_content = basic_dag_path.read_text().replace(
        "DBT_PROJECT_PATH = DBT_ROOT_PATH / DBT_PROJECT_NAME", f'DBT_PROJECT_PATH = Path("{test_path}")'
    )
    dag_file.write_text(dag_content)

    return test_path


@pytest.fixture
def test_dag_id():
    """DAG ID used in versioning tests."""
    return "basic_cosmos_dag"


@pytest.fixture
def dag_version_cleaner(test_dag_id):
    """Fixture to clean up DAG versioning data before and after tests."""

    @provide_session
    def _cleanup(dag_id, session=None):
        """Clean up all Airflow 3 versioning-related data for a DAG."""
        try:
            for table in [TaskInstance, DagRun, SerializedDagModel, DagVersion, DagCode, DagModel]:
                session.query(table).filter(table.dag_id == dag_id).delete()
            session.commit()
        except Exception:
            session.rollback()
            raise

    _cleanup(test_dag_id)
    yield
    _cleanup(test_dag_id)


@pytest.fixture
def serialize_dag():
    """Factory to serialize DAG."""

    @provide_session
    def _serialize(dag, bundle_name="test_bundle", bundle_version="1.0.0", session=None):
        """Serialize DAG."""
        # Ensure bundle exists
        if not session.get(DagBundleModel, bundle_name):
            session.add(DagBundleModel(name=bundle_name))
            session.flush()

        # Ensure DagModel exists
        if not session.get(DagModel, dag.dag_id):
            session.add(DagModel(dag_id=dag.dag_id, bundle_name=bundle_name))
            session.flush()

        # Serialize DAG (uses scheduler's hash-based versioning)
        return SerializedDagModel.write_dag(
            dag=LazyDeserializedDAG.from_dag(dag),
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            session=session,
        )

    return _serialize


@pytest.fixture
def task_instance_creator():
    """Factory to create task instances."""

    @provide_session
    def _create(dag, dag_version, session=None):
        """Create a DagRun and Success TaskInstance."""
        logical_date = timezone.utcnow()
        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_id=f"test_{logical_date.isoformat()}",
            logical_date=logical_date,
            state=DagRunState.SUCCESS,
            run_type="manual",
        )
        dag_run.dag_version = dag_version
        session.add(dag_run)
        session.flush()

        task = list(dag.tasks)[0]
        task_instance = TaskInstance(
            task=task,
            run_id=dag_run.run_id,
            dag_version_id=dag_version.id,
            state=TaskInstanceState.SUCCESS,
        )
        session.add(task_instance)
        session.commit()
        return task_instance

    return _create


@pytest.mark.integration
def test_cosmos_dag_version_tracking_with_added_model(
    jaffle_shop_test_dir, test_dag_id, dag_version_cleaner, serialize_dag, task_instance_creator
):
    """Test that DAG versions increment when dbt models are added.

    This test verifies that Airflow 3's DAG versioning system correctly tracks
    structural changes to Cosmos DAGs when dbt models are added.
    """
    # Parse DAG and serialize
    dagbag = DagBag(dag_folder=jaffle_shop_test_dir)
    dag_v1 = dagbag.dags[test_dag_id]

    serialize_dag(dag_v1)
    version_1 = DagVersion.get_latest_version(test_dag_id)

    task_instance_creator(dag_v1, version_1)

    # Add new dbt model (cosmos dag should change)
    (jaffle_shop_test_dir / "models" / "new_model.sql").write_text("select 1 as id;")

    # Re-parse DAG and serialize
    dagbag = DagBag(dag_folder=jaffle_shop_test_dir)
    dag_v2 = dagbag.dags[test_dag_id]

    serialize_dag(dag_v2)
    version_2 = DagVersion.get_latest_version(test_dag_id)

    # Verify version incremented
    assert version_1.version_number == 1
    assert version_2.version_number == 2
    assert version_1.id != version_2.id

    # Verify structural change (new task added)
    assert len(dag_v2.tasks) == len(dag_v1.tasks) + 1


@pytest.mark.integration
def test_cosmos_dag_version_unchanged_without_modifications(
    jaffle_shop_test_dir, test_dag_id, dag_version_cleaner, serialize_dag, task_instance_creator
):
    """Test that DAG version does not change when DAG structure is unchanged.

    This test verifies that Airflow 3's DAG versioning system recognizes
    when there are no structural changes to the DAG.
    """
    # Parse DAG and serialize
    dagbag = DagBag(dag_folder=jaffle_shop_test_dir)
    dag_v1 = dagbag.dags[test_dag_id]

    serialize_dag(dag_v1)
    version_1 = DagVersion.get_latest_version(test_dag_id)

    task_instance_creator(dag_v1, version_1)

    # Re-parse DAG and serialize
    dagbag = DagBag(dag_folder=jaffle_shop_test_dir)
    dag_v2 = dagbag.dags[test_dag_id]

    serialize_dag(dag_v2)
    version_2 = DagVersion.get_latest_version(test_dag_id)

    # Verify version did not increment (same version reused)
    assert version_1.id == version_2.id
    assert version_1.version_number == version_2.version_number

    # Verify same number of tasks
    assert len(dag_v2.tasks) == len(dag_v1.tasks)
