"""
Topology tests for DAGs containing adapter-native metric views (Databricks) and semantic views (Snowflake),
built from the manifests committed with the dev projects. No adapter or warehouse is needed; execution
against real warehouses lives in tests/e2e/test_metric_and_semantic_views.py.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from cosmos import (
    DbtDag,
    ExecutionConfig,
    ExecutionMode,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)
from cosmos import settings as cosmos_settings
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtRunLocalOperator,
    DbtSeedLocalOperator,
    DbtSemanticMetricViewLocalOperator,
    DbtTestLocalOperator,
)
from cosmos.operators.watcher import (
    DbtProducerWatcherOperator,
    DbtRunWatcherOperator,
    DbtSeedWatcherOperator,
    DbtSemanticMetricViewWatcherOperator,
    DbtTestWatcherOperator,
)

DBT_ROOT_PATH = Path(__file__).parent.parent.parent / "dev" / "dags" / "dbt"
# Identifiers introduced by PR #2885; kept in one place because the naming is under review.
RECLASSIFIED_RESOURCE_TYPE = "semantic_metric_view"
RECLASSIFIED_TASK_SUFFIX = "semantic_metric_run"
RECLASSIFIED_SETTING = "enable_semantic_metric_view_reclassification"
EXPECTED_NODE_COUNT = 9
PROJECTS = {
    "databricks": ("databricks_metric_view", "order_metrics"),
    "snowflake": ("snowflake_semantic_view", "orders_semantic_view"),
}


@pytest.fixture(params=sorted(PROJECTS), ids=sorted(PROJECTS))
def project(request, monkeypatch) -> tuple[str, str]:
    monkeypatch.setattr(cosmos_settings, RECLASSIFIED_SETTING, True)
    return PROJECTS[request.param]


def build_dag(
    project_name: str,
    dag_id: str,
    test_behavior: TestBehavior = TestBehavior.AFTER_EACH,
    execution_mode: ExecutionMode = ExecutionMode.LOCAL,
    select: list[str] | None = None,
    exclude: list[str] | None = None,
) -> DbtDag:
    project_dir = DBT_ROOT_PATH / project_name
    return DbtDag(
        project_config=ProjectConfig(
            dbt_project_path=project_dir,
            manifest_path=project_dir / "target" / "manifest.json",
            project_name=project_name,
        ),
        profile_config=ProfileConfig(
            profile_name=project_name,
            target_name="dev",
            profiles_yml_filepath=project_dir / "profiles.yml",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=test_behavior,
            select=select or [],
            exclude=exclude or [],
        ),
        execution_config=ExecutionConfig(execution_mode=execution_mode),
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
    )


def topological_task_ids(dag: DbtDag) -> list[str]:
    return [task.task_id for task in dag.topological_sort()]


def grouped_chain(semantic_model: str) -> list[str]:
    return [
        "raw_orders_seed",
        "orders.run",
        "orders.test",
        f"{semantic_model}.{RECLASSIFIED_TASK_SUFFIX}",
        f"{semantic_model}.test",
        "revenue_by_status.run",
        "revenue_by_status.test",
    ]


def test_after_each_topology(project):
    project_name, semantic_model = project
    dag = build_dag(project_name, dag_id=f"{project_name}_after_each")

    assert len(dag.dbt_graph.filtered_nodes) == EXPECTED_NODE_COUNT
    assert topological_task_ids(dag) == grouped_chain(semantic_model)
    assert isinstance(dag.task_dict["raw_orders_seed"], DbtSeedLocalOperator)
    assert isinstance(dag.task_dict["orders.run"], DbtRunLocalOperator)
    assert isinstance(dag.task_dict["orders.test"], DbtTestLocalOperator)
    assert isinstance(dag.task_dict["revenue_by_status.run"], DbtRunLocalOperator)
    semantic_task = dag.task_dict[f"{semantic_model}.{RECLASSIFIED_TASK_SUFFIX}"]
    assert isinstance(semantic_task, DbtSemanticMetricViewLocalOperator)
    assert semantic_task.base_cmd == ["run"]
    assert semantic_task.select == f"fqn:{project_name}.{semantic_model}"
    assert semantic_task.upstream_task_ids == {"orders.test"}
    assert semantic_task.downstream_task_ids == {f"{semantic_model}.test"}
    assert dag.task_dict[f"{semantic_model}.test"].downstream_task_ids == {"revenue_by_status.run"}


def test_watcher_topology(project):
    project_name, semantic_model = project
    dag = build_dag(project_name, dag_id=f"{project_name}_watcher", execution_mode=ExecutionMode.WATCHER)

    assert topological_task_ids(dag) == ["dbt_producer_watcher", *grouped_chain(semantic_model)]
    assert isinstance(dag.task_dict["dbt_producer_watcher"], DbtProducerWatcherOperator)
    assert isinstance(dag.task_dict["raw_orders_seed"], DbtSeedWatcherOperator)
    assert isinstance(dag.task_dict["orders.run"], DbtRunWatcherOperator)
    assert isinstance(dag.task_dict["orders.test"], DbtTestWatcherOperator)
    assert isinstance(
        dag.task_dict[f"{semantic_model}.{RECLASSIFIED_TASK_SUFFIX}"], DbtSemanticMetricViewWatcherOperator
    )
    assert "dbt_producer_watcher" in dag.task_dict["raw_orders_seed"].upstream_task_ids


DBT_BUILD_RESOURCE_TYPES = {"model", "snapshot", "seed"}


@pytest.mark.parametrize("test_behavior", [TestBehavior.NONE, TestBehavior.AFTER_ALL])
def test_watcher_producer_builds_reclassified_node_through_dbt_resource_types(project, test_behavior):
    """With tests excluded, the producer narrows `dbt build` with `--resource-type`. Those values go to
    dbt, which still classifies a metric view or semantic view as `model`, so the reclassified node is
    built by `--resource-type model`; Cosmos's own type must never leak into the command."""
    project_name, semantic_model = project
    dag = build_dag(
        project_name,
        dag_id=f"{project_name}_watcher_{test_behavior.value}",
        test_behavior=test_behavior,
        execution_mode=ExecutionMode.WATCHER,
    )
    producer = dag.task_dict["dbt_producer_watcher"]
    command, _ = producer.build_cmd({}, {})
    flags = [str(part) for part in command]
    resource_types = {flags[index + 1] for index, part in enumerate(flags) if part == "--resource-type"}

    assert resource_types == DBT_BUILD_RESOURCE_TYPES
    assert RECLASSIFIED_RESOURCE_TYPE not in flags
    assert f"{semantic_model}_{RECLASSIFIED_TASK_SUFFIX}" in dag.task_ids
    assert isinstance(
        dag.task_dict[f"{semantic_model}_{RECLASSIFIED_TASK_SUFFIX}"], DbtSemanticMetricViewWatcherOperator
    )


def test_build_topology(project):
    project_name, semantic_model = project
    dag = build_dag(project_name, dag_id=f"{project_name}_build", test_behavior=TestBehavior.BUILD)
    semantic_task_id = f"{semantic_model}_{RECLASSIFIED_RESOURCE_TYPE}_build"

    assert topological_task_ids(dag) == [
        "raw_orders_seed_build",
        "orders_model_build",
        semantic_task_id,
        "revenue_by_status_model_build",
    ]
    assert all(isinstance(task, DbtBuildLocalOperator) for task in dag.tasks)
    assert dag.task_dict[semantic_task_id].upstream_task_ids == {"orders_model_build"}
    assert dag.task_dict[semantic_task_id].downstream_task_ids == {"revenue_by_status_model_build"}


def test_after_all_topology(project):
    project_name, semantic_model = project
    dag = build_dag(project_name, dag_id=f"{project_name}_after_all", test_behavior=TestBehavior.AFTER_ALL)
    semantic_task_id = f"{semantic_model}_{RECLASSIFIED_TASK_SUFFIX}"

    assert topological_task_ids(dag) == [
        "raw_orders_seed",
        "orders_run",
        semantic_task_id,
        "revenue_by_status_run",
        f"{project_name}_test",
    ]
    assert isinstance(dag.task_dict[semantic_task_id], DbtSemanticMetricViewLocalOperator)
    assert isinstance(dag.task_dict[f"{project_name}_test"], DbtTestLocalOperator)


def test_select_by_reclassified_resource_type(project):
    project_name, semantic_model = project
    dag = build_dag(
        project_name, dag_id=f"{project_name}_select", select=[f"resource_type:{RECLASSIFIED_RESOURCE_TYPE}"]
    )
    assert set(dag.task_ids) == {f"{semantic_model}.{RECLASSIFIED_TASK_SUFFIX}", f"{semantic_model}.test"}


def test_exclude_by_reclassified_resource_type(project):
    project_name, semantic_model = project
    dag = build_dag(
        project_name, dag_id=f"{project_name}_exclude", exclude=[f"resource_type:{RECLASSIFIED_RESOURCE_TYPE}"]
    )
    assert set(dag.task_ids) == {
        "raw_orders_seed",
        "orders.run",
        "orders.test",
        "revenue_by_status.run",
        "revenue_by_status.test",
    }


def test_select_models_excludes_reclassified_node(project):
    project_name, semantic_model = project
    dag = build_dag(project_name, dag_id=f"{project_name}_select_models", select=["resource_type:model"])
    assert f"{semantic_model}.{RECLASSIFIED_TASK_SUFFIX}" not in dag.task_ids
    assert "orders.run" in dag.task_ids


def test_reclassification_disabled_restores_run_task(project, monkeypatch):
    project_name, semantic_model = project
    monkeypatch.setattr(cosmos_settings, RECLASSIFIED_SETTING, False)
    dag = build_dag(project_name, dag_id=f"{project_name}_reclassification_disabled")

    assert topological_task_ids(dag) == [
        "raw_orders_seed",
        "orders.run",
        "orders.test",
        f"{semantic_model}.run",
        f"{semantic_model}.test",
        "revenue_by_status.run",
        "revenue_by_status.test",
    ]
    assert isinstance(dag.task_dict[f"{semantic_model}.run"], DbtRunLocalOperator)
