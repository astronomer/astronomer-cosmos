from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.eks import EksHook

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

from cosmos.operators.kubernetes import (
    DbtBuildKubernetesOperator,
    DbtCloneKubernetesOperator,
    DbtKubernetesBaseOperator,
    DbtLSKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtRunOperationKubernetesOperator,
    DbtSeedKubernetesOperator,
    DbtSnapshotKubernetesOperator,
    DbtSourceKubernetesOperator,
    DbtTestKubernetesOperator,
)

DEFAULT_CONN_ID = "aws_default"
DEFAULT_NAMESPACE = "default"


class DbtAwsEksBaseOperator(DbtKubernetesBaseOperator):
    template_fields: Sequence[str] = tuple(
        {
            "cluster_name",
            "in_cluster",
            "namespace",
            "pod_name",
            "aws_conn_id",
            "region",
        }
        | set(DbtKubernetesBaseOperator.template_fields)
    )

    def __init__(
        self,
        cluster_name: str,
        pod_name: str | None = None,
        namespace: str | None = DEFAULT_NAMESPACE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.cluster_name = cluster_name
        self.pod_name = pod_name
        self.namespace = namespace
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(
            name=self.pod_name,
            namespace=self.namespace,
            **kwargs,
        )
        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the EksPodOperator.
        if self.config_file:
            raise AirflowException("The config_file is not an allowed parameter for the EksPodOperator.")

    def execute(self, context: Context) -> Any | None:  # type: ignore
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )
        with eks_hook.generate_config_file(
            eks_cluster_name=self.cluster_name, pod_namespace=self.namespace
        ) as self.config_file:
            return super().execute(context)


class DbtBuildAwsEksOperator(DbtAwsEksBaseOperator, DbtBuildKubernetesOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = (
        DbtAwsEksBaseOperator.template_fields + DbtBuildKubernetesOperator.template_fields  # type: ignore[operator]
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSAwsEksOperator(DbtAwsEksBaseOperator, DbtLSKubernetesOperator):
    """
    Executes a dbt core ls command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedAwsEksOperator(DbtAwsEksBaseOperator, DbtSeedKubernetesOperator):
    """
    Executes a dbt core seed command.
    """

    template_fields: Sequence[str] = (
        DbtAwsEksBaseOperator.template_fields + DbtSeedKubernetesOperator.template_fields  # type: ignore[operator]
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotAwsEksOperator(DbtAwsEksBaseOperator, DbtSnapshotKubernetesOperator):
    """
    Executes a dbt core snapshot command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceAzureContainerInstanceOperator(DbtAwsEksBaseOperator, DbtSourceKubernetesOperator):
    """
    Executes a dbt source freshness command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunAwsEksOperator(DbtAwsEksBaseOperator, DbtRunKubernetesOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = (
        DbtAwsEksBaseOperator.template_fields + DbtRunKubernetesOperator.template_fields  # type: ignore[operator]
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestAwsEksOperator(DbtAwsEksBaseOperator, DbtTestKubernetesOperator):
    """
    Executes a dbt core test command.
    """

    template_fields: Sequence[str] = (
        DbtAwsEksBaseOperator.template_fields + DbtTestKubernetesOperator.template_fields  # type: ignore[operator]
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunOperationAwsEksOperator(DbtAwsEksBaseOperator, DbtRunOperationKubernetesOperator):
    """
    Executes a dbt core run-operation command.
    """

    template_fields: Sequence[str] = (
        DbtAwsEksBaseOperator.template_fields + DbtRunOperationKubernetesOperator.template_fields  # type: ignore[operator]
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtCloneAwsEksOperator(DbtAwsEksBaseOperator, DbtCloneKubernetesOperator):
    """
    Executes a dbt core clone command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
