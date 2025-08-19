from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, Sequence

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from cosmos.config import RenderConfig
from cosmos.constants import InvocationMode
from cosmos.operators.local import DbtBuildLocalOperator

try:
    from dbt_common.events.base_types import EventMsg  # dbt ≥ 1.7
except ImportError:  # pragma: no cover
    EventMsg = None  # type: ignore

logger = logging.getLogger(__name__)

__all__: Sequence[str] = [
    "DbtMasterBuildOperator",
    "create_model_result_task",
]


class DbtMasterBuildOperator(DbtBuildLocalOperator):
    """Run a single ``dbt build`` that materialises **all** selected resources.

    After completion it parses ``target/run_results.json`` and pushes the status
    of each node to XCom (key ``run_results``).  Down-stream lightweight tasks
    can then decide success / failure per model without running dbt again.

    This operator keeps the heavy work in one place while still allowing fine-
    grained model-level tasks in the Airflow DAG.
    """

    # template_fields: Sequence[str] = ("select",)

    def __init__(
        self,
        *,
        render_config: RenderConfig | None = None,
        **kwargs: Any,
    ) -> None:
        """Create a master build task.

        Parameters
        ----------
        select
            Optional list of dbt selection strings. Models outside this list are skipped.
        render_config
            The same ``RenderConfig`` instance used by the DAG converter.  It is
            inspected here so we can honour ``test_behavior`` and pick the right
            dbt sub-command (``build`` vs ``run``).
        """

        # from cosmos.config import RenderConfig  # local import to avoid circular

        self.render_config: RenderConfig | None = render_config

        task_id = kwargs.pop("task_id", "dbt_master_build")
        super().__init__(task_id=task_id, **kwargs)

    def _choose_base_cmd(self) -> list[str]:
        # if self.render_config is None:
        #     return ["build"]  # Fallback
        #
        # from cosmos.constants import TestBehavior

        # tb = self.render_config.test_behavior
        # if tb == TestBehavior.AFTER_ALL:
        #     return ["build"]  # run + test

        return ["build"]

    @property
    def base_cmd(self) -> list[str]:
        return self._choose_base_cmd()

    def add_cmd_flags(self) -> list[str]:
        flags: list[str] = super().add_cmd_flags()

        logger.info("DbtMasterBuildOperator: render_config: %s", self.render_config)
        if self.render_config is not None and self.render_config.exclude:
            logger.info("DbtMasterBuildOperator: adding exclude flags: %s", self.render_config.exclude)
            flags.extend(["--exclude", *self.render_config.exclude])

        if self.render_config is not None and self.render_config.select:
            logger.info("DbtMasterBuildOperator: adding select flags: %s", self.render_config.select)
            flags.extend(["--select", *self.render_config.select])

        # flags += ["--log-format", "json"]
        return flags

    def execute(self, context: Context, **kwargs: Any) -> Any:  # type: ignore[override]
        # use_events = self.invocation_mode == InvocationMode.DBT_RUNNER
        use_events = self.invocation_mode == InvocationMode.DBT_RUNNER and EventMsg is not None

        use_events = True
        results_mapping: dict[str, str] = {}

        if use_events:
            logger.info("DbtMasterBuildOperator: capturing node statuses via dbtRunner callbacks")

            ti = context["ti"]  # we’ll need it inside the callback

            def _event_callback(ev: EventMsg) -> None:  # type: ignore[valid-type]
                """
                Called for EVERY structured dbt event.  We only care about model/node-level completion messages.
                """

                if ev.info.name == "NodeFinished":  # adjust if dbt changes the name
                    uid = ev.data.node_info.unique_id
                    status = ev.data.run_result.status.upper()
                    # logger.info("uid: %s, status: %s", uid, status)
                    results_mapping[uid] = status
                    # push immediately so watcher tasks can finish in parallel
                    ti.xcom_push(key=f"status_{uid}", value=status)

            import cosmos.dbt.runner as _dbt_runner_mod

            original_get_runner = _dbt_runner_mod.get_runner

            def _patched_get_runner():  # type: ignore[override]
                from dbt.cli.main import dbtRunner

                return dbtRunner(callbacks=[_event_callback])

            # Install the patch and clear any cached runner
            _dbt_runner_mod.get_runner = _patched_get_runner  # type: ignore[assignment]
            if hasattr(original_get_runner, "cache_clear"):
                original_get_runner.cache_clear()  # remove cached plain runner

            try:
                # This goes through AbstractDbtLocalBase.run_command which
                # copies the project to a temp dir, builds all flags, etc.
                super().execute(context=context, **kwargs)
            finally:
                _dbt_runner_mod.get_runner = original_get_runner

            logger.info("Captured %d node statuses from dbtRunner events", len(results_mapping))

        else:
            logger.info("DbtMasterBuildOperator: falling back to run_results.json for status capture")
            super().execute(context=context, **kwargs)

            run_results_path = Path(self.project_dir) / "target" / "run_results.json"
            if not run_results_path.is_file():
                raise AirflowException(f"run_results.json not found at {run_results_path}")

            try:
                with run_results_path.open() as fp:
                    raw = json.load(fp)
            except json.JSONDecodeError as exc:  # pragma: no cover
                raise AirflowException("Invalid JSON in run_results.json") from exc

            results_mapping = {r["unique_id"]: r.get("status", "unknown") for r in raw.get("results", [])}
            logger.info("Parsed %d entries out of run_results.json", len(results_mapping))

        # Push mapping to XCom so downstream sensors can consume
        context["ti"].xcom_push(key="run_results", value=results_mapping)
        return results_mapping


class DbtModelStatusSensor(BaseSensorOperator):
    template_fields = ("model_unique_id",)

    def __init__(
        self,
        *,
        model_unique_id: str,
        master_task_id: str = "dbt_master_build",
        check_fn: Callable[[str], bool] | None = None,
        poke_interval: int = 20,
        timeout: int = 60 * 60,  # 1 h safety valve
        **kwargs: Any,
    ) -> None:
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.model_unique_id = model_unique_id
        self.master_task_id = master_task_id
        self.check_fn = check_fn or (lambda s: s == "SUCCESS")

    def poke(self, context: Context) -> bool:
        ti = context["ti"]
        # 1) immediate per-node XCom
        logger.info(
            "DbtModelStatusSensor: pulling status from task_id=%s, xcom, key=%s",
            self.master_task_id,
            f"status_{self.model_unique_id}",
        )
        status = ti.xcom_pull(task_ids=self.master_task_id, key=f"status_{self.model_unique_id}")

        # 2) final summary mapping as fallback
        if status is None:
            mapping = ti.xcom_pull(task_ids=self.master_task_id, key="run_results")
            status = mapping.get(self.model_unique_id) if mapping else None

        if status is None:
            return False

        self.log.info("Model %s finished with status %s", self.model_unique_id, status)
        if not self.check_fn(status):
            raise AirflowException(f"Model {self.model_unique_id} finished with status '{status}'")
        return True


def create_model_result_task(
    *,
    dag: DAG,
    model_unique_id: str,
    upstream_task_id: str = "dbt_master_build",
    check_fn: Callable[[str], bool] | None = None,
    **sensor_kwargs: Any,
) -> BaseSensorOperator:
    """
    Return a sensor that waits for the model’s status XCom pushed by the
    master build task.
    """
    task_id = f"{model_unique_id.split('.')[-1]}"
    return DbtModelStatusSensor(
        task_id=task_id,
        dag=dag,
        model_unique_id=model_unique_id,
        master_task_id=upstream_task_id,
        check_fn=check_fn,
        retries=0,
        **sensor_kwargs,
    )
