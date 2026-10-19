import importlib

import pytest

from cosmos.operators.base import DbtRunMixin, DbtSemanticMetricViewMixin

# (module, class name, minimal constructor kwargs) for the semantic metric view operator of each execution mode.
SEMANTIC_METRIC_VIEW_OPERATORS = [
    ("local", "DbtSemanticMetricViewLocalOperator", {"profile_config": None}),
    ("virtualenv", "DbtSemanticMetricViewVirtualenvOperator", {"profile_config": None, "py_requirements": []}),
    ("docker", "DbtSemanticMetricViewDockerOperator", {"image": "my_image"}),
    ("kubernetes", "DbtSemanticMetricViewKubernetesOperator", {"image": "my_image"}),
    ("aws_eks", "DbtSemanticMetricViewAwsEksOperator", {"image": "my_image", "cluster_name": "my_cluster"}),
    (
        "aws_ecs",
        "DbtSemanticMetricViewAwsEcsOperator",
        {"aws_conn_id": "aws", "cluster": "c", "task_definition": "d", "container_name": "n"},
    ),
    (
        "azure_container_instance",
        "DbtSemanticMetricViewAzureContainerInstanceOperator",
        {"ci_conn_id": "c", "resource_group": "r", "name": "n", "image": "my_image", "region": "x"},
    ),
    (
        "gcp_gke",
        "DbtSemanticMetricViewGcpGkeOperator",
        {"image": "my_image", "project_id": "p", "location": "l", "cluster_name": "c"},
    ),
    (
        "gcp_cloud_run_job",
        "DbtSemanticMetricViewGcpCloudRunJobOperator",
        {"project_id": "p", "region": "r", "job_name": "j"},
    ),
    ("airflow_async", "DbtSemanticMetricViewAirflowAsyncOperator", {"profile_config": None}),
    ("watcher", "DbtSemanticMetricViewWatcherOperator", {"profile_config": None}),
    ("watcher_kubernetes", "DbtSemanticMetricViewWatcherKubernetesOperator", {"image": "my_image"}),
    (
        "watcher_gcp_gke",
        "DbtSemanticMetricViewWatcherGcpGkeOperator",
        {"image": "my_image", "project_id": "p", "location": "l", "cluster_name": "c"},
    ),
]


@pytest.mark.parametrize(
    "module_name, class_name, kwargs",
    SEMANTIC_METRIC_VIEW_OPERATORS,
    ids=[module_name for module_name, _, _ in SEMANTIC_METRIC_VIEW_OPERATORS],
)
def test_semantic_metric_view_operator_runs_dbt_run_with_distinct_ui_color(module_name, class_name, kwargs):
    """Every execution mode exposes a DbtSemanticMetricView operator that still runs `dbt run`,
    but renders with its own color so semantic metric views stand out from regular models."""
    operator_class = getattr(importlib.import_module(f"cosmos.operators.{module_name}"), class_name)

    operator = operator_class(task_id="my_task", project_dir="my/dir", **kwargs)

    assert isinstance(operator, DbtSemanticMetricViewMixin)
    assert operator.base_cmd == ["run"]
    assert operator.ui_color == DbtSemanticMetricViewMixin.ui_color
    assert operator.ui_color != DbtRunMixin.ui_color
