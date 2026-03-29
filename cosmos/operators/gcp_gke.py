from __future__ import annotations

from abc import ABC
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator
except ImportError:
    raise ImportError(
        "Could not import GKEStartPodOperator. Ensure you've installed the Google Cloud provider "
        "separately or with `pip install astronomer-cosmos[google]`."
    )

import cosmos.operators._k8s_common as _k8s_common
from cosmos.config import ProfileConfig
from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtCloneMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
)


class DbtGcpGkeBaseOperator(AbstractDbtBase, GKEStartPodOperator):  # type: ignore
    """
    Executes a dbt core cli command in a GKE Pod.
    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) + list(GKEStartPodOperator.template_fields)
    )

    intercept_flag = False

    def __init__(self, profile_config: ProfileConfig | None = None, **kwargs: Any) -> None:
        _k8s_common.init_k8s_operator(self, GKEStartPodOperator, profile_config, kwargs)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        _k8s_common.build_and_run_cmd(self, GKEStartPodOperator, context, cmd_flags)

    def build_kube_args(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        _k8s_common.build_kube_args(self, context, cmd_flags)


class DbtWarningGcpGkeOperator(DbtGcpGkeBaseOperator, ABC):
    """
    Base for dbt operators that detect and handle test/source freshness warnings.
    """

    def __init__(self, *args: Any, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.warning_handler = _k8s_common.setup_warning_handler(
            self, on_warning_callback, DbtTestGcpGkeOperator, DbtSourceGcpGkeOperator
        )

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        if self.warning_handler:
            self.warning_handler.context = context
        super().build_and_run_cmd(
            context=context, cmd_flags=cmd_flags, run_as_async=run_as_async, async_context=async_context
        )


class DbtBuildGcpGkeOperator(DbtBuildMixin, DbtGcpGkeBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtGcpGkeBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]


class DbtLSGcpGkeOperator(DbtLSMixin, DbtGcpGkeBaseOperator):
    """
    Executes a dbt core ls command.
    """


class DbtSeedGcpGkeOperator(DbtSeedMixin, DbtGcpGkeBaseOperator):
    """
    Executes a dbt core seed command.
    """

    template_fields: Sequence[str] = DbtGcpGkeBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]


class DbtSnapshotGcpGkeOperator(DbtSnapshotMixin, DbtGcpGkeBaseOperator):
    """
    Executes a dbt core snapshot command.
    """


class DbtTestGcpGkeOperator(DbtTestMixin, DbtWarningGcpGkeOperator):
    """
    Executes a dbt core test command.
    """


class DbtSourceGcpGkeOperator(DbtSourceMixin, DbtWarningGcpGkeOperator):
    """
    Executes a dbt source freshness command.
    """


class DbtRunGcpGkeOperator(DbtRunMixin, DbtGcpGkeBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtGcpGkeBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]


class DbtRunOperationGcpGkeOperator(DbtRunOperationMixin, DbtGcpGkeBaseOperator):
    """
    Executes a dbt core run-operation command.
    """

    template_fields: Sequence[str] = DbtGcpGkeBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]


class DbtCloneGcpGkeOperator(DbtCloneMixin, DbtGcpGkeBaseOperator):
    """
    Executes a dbt core clone command.
    """
