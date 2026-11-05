import importlib

import pytest

from cosmos._utils.importer import load_method_from_module
from cosmos.airflow.graph import calculate_operator_class
from cosmos.constants import ExecutionMode
from cosmos.operators._asynchronous.base import DbtRunAirflowAsyncFactoryOperator
from cosmos.operators.base import DbtRunMixin, DbtSemanticMetricViewMixin
from cosmos.operators.local import DbtSemanticMetricViewLocalOperator

try:
    from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator  # noqa: F401

    GOOGLE_PROVIDER_AVAILABLE = True
except ImportError:
    GOOGLE_PROVIDER_AVAILABLE = False

requires_google_provider = pytest.mark.skipif(
    not GOOGLE_PROVIDER_AVAILABLE, reason="Google Cloud provider not importable"
)
GKE_MODULES = {"gcp_gke", "watcher_gcp_gke"}

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
    [
        pytest.param(*case, id=case[0], marks=requires_google_provider if case[0] in GKE_MODULES else [])
        for case in SEMANTIC_METRIC_VIEW_OPERATORS
    ],
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


def test_semantic_metric_view_airflow_async_operator_runs_synchronously_like_other_non_model_resources():
    """Only `DbtRun` has an async operator (BigQuery only), so non-model resources, semantic metric views included,
    run on the worker like in LOCAL mode. Pin that, so it can't silently become an async operator."""
    operator_path = calculate_operator_class(ExecutionMode.AIRFLOW_ASYNC, "DbtSemanticMetricView")

    operator_class = load_method_from_module(*operator_path.rsplit(".", 1))

    assert issubclass(operator_class, DbtSemanticMetricViewLocalOperator)
    assert not issubclass(operator_class, DbtRunAirflowAsyncFactoryOperator)
