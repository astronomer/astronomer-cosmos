from __future__ import annotations

import importlib.metadata
import logging
import warnings
from typing import Any

import sqlalchemy
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION, DBT_DISTRIBUTION_NAMES

log = logging.getLogger(__name__)


def make_dag_bag(*args: Any, **kwargs: Any) -> Any:
    """Construct a ``DagBag``, dropping ``include_examples`` on Airflow >= 3.3.

    Airflow 3.3 removed the ``include_examples`` kwarg from ``DagBag`` as part of
    AIP-66 ("provider example DAGs as dedicated bundles", apache/airflow#66161);
    example loading is now gated by ``[core] load_examples``. Pointing
    ``dag_folder`` at a directory still loads only that directory, so dropping the
    kwarg preserves the test's intent. Also tracks the import-location move to
    ``airflow.dag_processing.dagbag`` in Airflow 3.1+.
    """
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:
        from airflow.models.dagbag import DagBag

    # Compare the release tuple, not the Version: a pre-release sorts below its
    # final release (PEP 440), so Version("3.3.0b2") >= Version("3.3") is False and
    # would wrongly keep the kwarg on 3.3 betas.
    if AIRFLOW_VERSION.release[:2] >= (3, 3):
        kwargs.pop("include_examples", None)
    return DagBag(*args, **kwargs)


def run_dag(dag: DAG, expected_dag_state: DagRunState = DagRunState.SUCCESS) -> DagRun:
    return test_dag(dag=dag, expected_dag_state=expected_dag_state)


def check_dag_state(dag_run: DagRun | None, expected_dag_state: DagRunState = DagRunState.SUCCESS) -> bool:
    """Check if a DAG reached the expected state, if that Airflow version allows it."""
    if dag_run is not None:
        return dag_run.state == expected_dag_state
    return True


def serialize_dag_to_db(dag: DAG) -> None:
    """Serialize ``dag`` to the metadata DB.

    Airflow 3.1+ requires a DAG to be serialized before ``dag.test()`` / ``create_dagrun()``,
    which check for DagVersion and DagModel records. No-op on older Airflow.
    """
    if AIRFLOW_VERSION.release[:2] < (3, 1):
        return

    try:
        from airflow.dag_processing.dagbag import sync_bag_to_db
    except ImportError:
        from airflow.models.dagbag import sync_bag_to_db

    from airflow.models.dagbundle import DagBundleModel
    from airflow.utils.session import create_session

    # Create the DagBundle the DagModel foreign key needs (mimics manager.sync_bundles_to_db()),
    # then bag + sync to create the DagModel and DagVersion records.
    with create_session() as session:
        session.merge(DagBundleModel(name="test_bundle"))
        session.commit()

    dagbag = make_dag_bag(include_examples=False)
    dagbag.bag_dag(dag)
    sync_bag_to_db(dagbag, bundle_name="test_bundle", bundle_version="1")


def new_test_dag(dag: DAG, expected_dag_state: DagRunState = DagRunState.SUCCESS) -> DagRun:
    if AIRFLOW_VERSION.release[:2] >= (3, 1):
        serialize_dag_to_db(dag)
        dr = dag.test(logical_date=timezone.utcnow())
    elif AIRFLOW_VERSION.release[:2] >= (3, 0):
        # In Airflow 3.0, dag.test() does not properly register Assets as active, must be registered manually
        from airflow.models.asset import AssetActive, AssetModel
        from airflow.utils.session import create_session
        from sqlalchemy import select

        with create_session() as session:
            # Loop through each Task in the DAG, looking for outlets
            for task in dag.tasks:
                for outlet in getattr(task, "outlets", []):
                    if not hasattr(outlet, "uri"):
                        continue  # Skip the outlet

                    name = getattr(outlet, "name", outlet.uri)
                    uri = outlet.uri

                    # Retrieve Assets from DB to validate if they exist
                    asset_model = session.execute(
                        select(AssetModel).where(AssetModel.name == name, AssetModel.uri == uri)
                    ).scalar_one_or_none()

                    if asset_model is None:
                        # Add the Asset
                        asset_model = AssetModel(name=name, uri=uri)
                        session.add(asset_model)
                        session.flush()

                    if asset_model.active is None:
                        # Set the Asset as "active"
                        session.add(AssetActive.for_asset(asset_model))
                        session.flush()

        dr = dag.test(logical_date=timezone.utcnow())
    else:
        dr = dag.test()
    assert check_dag_state(
        dr, expected_dag_state
    ), f"Dag {dag.dag_id} did not reach expected state {expected_dag_state}. State: {dr.state}."
    return dr


def test_dag(
    dag,
    expected_dag_state: DagRunState = DagRunState.SUCCESS,
) -> DagRun:
    dr = None
    if AIRFLOW_VERSION.major >= 3:
        dr = new_test_dag(dag, expected_dag_state=expected_dag_state)
        assert check_dag_state(
            dr, expected_dag_state
        ), f"Dag {dag.dag_id} did not reach expected state {expected_dag_state}. State: {dr.state}. "
    else:
        # This is a work around until we fix the issue in Airflow:
        # https://github.com/apache/airflow/issues/42495
        """
        FAILED tests/test_example_dags.py::test_example_dag[example_model_version] - sqlalchemy.exc.PendingRollbackError:
        This Session's transaction has been rolled back due to a previous exception during flush. To begin a new transaction with this Session, first issue Session.rollback().
        Original exception was: Can't flush None value found in collection DatasetModel.aliases (Background on this error at: https://sqlalche.me/e/14/7s2a)
        FAILED tests/test_example_dags.py::test_example_dag[basic_cosmos_dag]
        FAILED tests/test_example_dags.py::test_example_dag[cosmos_profile_mapping]
        FAILED tests/test_example_dags.py::test_example_dag[user_defined_profile]
        """
        try:
            dr = new_test_dag(dag, expected_dag_state=expected_dag_state)
            assert check_dag_state(
                dr, expected_dag_state
            ), f"Dag {dag.dag_id} did not reach expected state {expected_dag_state}. State: {dr.state}. "
        except sqlalchemy.exc.PendingRollbackError:
            warnings.warn("Airflow 2 has issues when running the test command with DatasetAlias / Datasets")
    return dr


def get_dbt_version() -> Version:
    """Return the installed dbt version, for dbt-core 1.x and dbt-oss (dbt-oss has no ``dbt.version`` module).

    Assumes a single dbt install: it returns the first of dbt-core, dbt-oss and dbt that is installed, while
    ``is_dbt_v2()`` treats any of them at major version 2 as dbt 2, so the two disagree if dbt-core 1.x is installed
    next to dbt-oss.
    """
    for distribution in DBT_DISTRIBUTION_NAMES:
        try:
            return Version(importlib.metadata.version(distribution))
        except importlib.metadata.PackageNotFoundError:
            continue
    raise RuntimeError(f"None of {DBT_DISTRIBUTION_NAMES} is installed.")
