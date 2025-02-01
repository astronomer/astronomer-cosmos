from __future__ import annotations

from typing import Any, Sequence

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.context import Context

from cosmos.config import ProfileConfig
from cosmos.operators.local import AbstractDbtLocalBase


class DbtRunAirflowAsyncBigqueryOperator(BigQueryInsertJobOperator, AbstractDbtLocalBase):  # type: ignore[misc]

    template_fields: Sequence[str] = (
        "full_refresh",
        "gcp_project",
        "dataset",
        "location",
    )

    def __init__(
        self,
        project_dir: str,
        profile_config: ProfileConfig,
        extra_context: dict[str, Any] | None = None,
        dbt_kwargs={},
        **kwargs: Any,
    ):
        self.project_dir = project_dir
        self.profile_config = profile_config
        self.gcp_conn_id = self.profile_config.profile_mapping.conn_id  # type: ignore
        profile = self.profile_config.profile_mapping.profile  # type: ignore
        self.gcp_project = profile["project"]
        self.dataset = profile["dataset"]
        self.extra_context = extra_context or {}
        self.full_refresh = None
        if "full_refresh" in kwargs:
            self.full_refresh = kwargs.pop("full_refresh")
        self.configuration: dict[str, Any] = {}
        task_id = dbt_kwargs.pop("task_id")
        AbstractDbtLocalBase.__init__(
            self, task_id=task_id, project_dir=project_dir, profile_config=profile_config, **dbt_kwargs
        )
        super().__init__(
            gcp_conn_id=self.gcp_conn_id,
            configuration=self.configuration,
            deferrable=True,
            **kwargs,
        )
        self.dbt_kwargs = dbt_kwargs
        self.async_context = extra_context
        self.async_context["profile_type"] = self.profile_config.get_profile_type()
        self.async_context["async_operator"] = BigQueryInsertJobOperator

    @property
    def base_cmd(self) -> list[str]:
        return ["run"]

    def execute(self, context: Context) -> None:

        self.build_and_run_cmd(context=context, run_as_async=True, async_context=self.async_context)
        # super().execute(context)
