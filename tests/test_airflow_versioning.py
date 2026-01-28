import shutil
from pathlib import Path

import pytest
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION
from tests.utils import run_dag

if AIRFLOW_VERSION < Version("3.1"):
    pytest.skip("Skipping Airflow versioning tests on Airflow 2.x and 3.0", allow_module_level=True)
else:
    from airflow.models import DagBag, DagRun, TaskInstance
    from airflow.models.dag import DagModel
    from airflow.models.dag_version import DagVersion
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dagcode import DagCode
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.serialized_objects import LazyDeserializedDAG
    from airflow.utils.session import provide_session


DBT_PROJECT_PATH = Path(__file__).parent.parent / "dev" / "dags" / "dbt" / "simple"


@pytest.fixture
def dbt_project_test_dir(tmp_path):
    """Create a test directory with the local simple dbt project and DAG file."""
    test_path = tmp_path / "simple"
    shutil.copytree(DBT_PROJECT_PATH, test_path, dirs_exist_ok=True)

    dag_file = test_path / "basic_cosmos_dag.py"
    basic_dag_path = Path(__file__).parent.parent / "dev" / "dags" / "basic_cosmos_dag.py"
    dag_content = (
        basic_dag_path.read_text()
        .replace("DBT_PROJECT_PATH = DBT_ROOT_PATH / DBT_PROJECT_NAME", f'DBT_PROJECT_PATH = Path("{test_path}")')
        .replace(
            'DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")',
            'DBT_PROJECT_NAME = "simple"',
        )
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
                session.query(table).filter(table.dag_id == dag_id).delete(synchronize_session="fetch")
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
        # Ensure bundle exists atomically
        session.merge(DagBundleModel(name=bundle_name))
        # Ensure DagModel exists atomically
        session.merge(DagModel(dag_id=dag.dag_id, bundle_name=bundle_name))

        # Serialize DAG (uses scheduler's hash-based versioning)
        return SerializedDagModel.write_dag(
            dag=LazyDeserializedDAG.from_dag(dag),
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            session=session,
        )

    return _serialize


@pytest.mark.integration
def test_cosmos_dag_version_tracking_with_added_model(
    dbt_project_test_dir, test_dag_id, dag_version_cleaner, serialize_dag
):
    """Test that DAG versions increment when dbt models are added.

    This test verifies that Airflow 3's DAG versioning system correctly tracks
    structural changes to Cosmos DAGs when dbt models are added.
    """
    # Parse DAG and run
    dagbag = DagBag(dag_folder=dbt_project_test_dir)
    dag_v1 = dagbag.dags[test_dag_id]
    run_dag(dag_v1)

    version_1 = DagVersion.get_latest_version(test_dag_id)

    # Add new dbt model (cosmos dag should change)
    (dbt_project_test_dir / "models" / "dummy_model.sql").write_text("SELECT 2 AS id, 'example' AS name")

    # Re-parse DAG, serialize and run
    dagbag = DagBag(dag_folder=dbt_project_test_dir)
    dag_v2 = dagbag.dags[test_dag_id]
    serialize_dag(dag_v2)
    run_dag(dag_v2)

    version_2 = DagVersion.get_latest_version(test_dag_id)

    # Verify version incremented
    assert version_1.version_number == 1
    assert version_2.version_number == 2

    # Verify structural change (new task added)
    assert len(dag_v2.tasks) == len(dag_v1.tasks) + 1


@pytest.mark.integration
def test_cosmos_dag_version_unchanged_without_modifications(
    dbt_project_test_dir,
    test_dag_id,
    dag_version_cleaner,
    serialize_dag,
):
    """Test that DAG version does not change when DAG structure is unchanged.

    This test verifies that Airflow 3's DAG versioning system recognizes
    when there are no structural changes to the DAG.
    """
    # Parse DAG and run
    dagbag = DagBag(dag_folder=dbt_project_test_dir)
    dag_v1 = dagbag.dags[test_dag_id]
    run_dag(dag_v1)

    version_1 = DagVersion.get_latest_version(test_dag_id)

    # Re-parse DAG, serialize and run
    dagbag = DagBag(dag_folder=dbt_project_test_dir)
    dag_v2 = dagbag.dags[test_dag_id]
    serialize_dag(dag_v2)
    run_dag(dag_v2)

    version_2 = DagVersion.get_latest_version(test_dag_id)

    # Verify version did not increment (same version reused)
    assert version_1.id == version_2.id
    assert version_1.version_number == version_2.version_number

    # Verify same number of tasks
    assert len(dag_v2.tasks) == len(dag_v1.tasks)


@pytest.mark.integration
def test_cosmos_dag_version_unchanged_with_non_structural_changes(
    dbt_project_test_dir,
    test_dag_id,
    dag_version_cleaner,
    serialize_dag,
):
    """Test that DAG version does not change when non-structural files change.

    This test verifies that Airflow 3's DAG versioning system correctly identifies
    that changes to non-structural files (like README or adding unrelated files)
    should not create a new DAG version. The DAG structure (tasks, dependencies)
    remains the same.
    """
    # Parse DAG and run
    dagbag = DagBag(dag_folder=dbt_project_test_dir)
    dag_v1 = dagbag.dags[test_dag_id]
    run_dag(dag_v1)

    version_1 = DagVersion.get_latest_version(test_dag_id)

    # Add a README file to the project (should NOT affect DAG structure)
    readme_file = dbt_project_test_dir / "README.md"
    readme_file.write_text("# My dbt Project\n\nThis is a test project.")

    # Add a random data file (should NOT affect DAG structure)
    data_file = dbt_project_test_dir / "data" / "sample.csv"
    data_file.parent.mkdir(exist_ok=True)
    data_file.write_text("id,name\n1,test\n2,example")

    # Re-parse DAG, serialize and run
    dagbag = DagBag(dag_folder=dbt_project_test_dir)
    dag_v2 = dagbag.dags[test_dag_id]
    serialize_dag(dag_v2)
    run_dag(dag_v2)

    version_2 = DagVersion.get_latest_version(test_dag_id)

    # Verify version did NOT increment (non-structural changes don't affect DAG)
    assert version_1.id == version_2.id
    assert version_1.version_number == version_2.version_number

    # Verify same number of tasks
    assert len(dag_v2.tasks) == len(dag_v1.tasks)
