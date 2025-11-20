from __future__ import annotations

import asyncio
import base64
import json
import zlib
from typing import Any, AsyncIterator

import airflow
from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async
from packaging.version import Version

AIRFLOW_VERSION = Version(airflow.__version__)


class WatcherTrigger(BaseTrigger):

    def __init__(
        self,
        model_unique_id: str,
        producer_task_id: str,
        dag_id: str,
        run_id: str,
        map_index: int | None,
        use_event: bool,
        poke_interval: float = 5.0,
    ):
        self.model_unique_id = model_unique_id
        self.producer_task_id = producer_task_id
        self.dag_id = dag_id
        self.run_id = run_id
        self.map_index = map_index
        self.use_event = use_event
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "cosmos._triggers.watcher.WatcherTrigger",
            {
                "model_unique_id": self.model_unique_id,
                "producer_task_id": self.producer_task_id,
                "dag_id": self.dag_id,
                "run_id": self.run_id,
                "map_index": self.map_index,
                "use_event": self.use_event,
                "poke_interval": self.poke_interval,
            },
        )

    async def get_xcom_val_af3(self, key: str) -> Any | None:
        from airflow.sdk.execution_time.xcom import XCom

        return await sync_to_async(XCom.get_one)(
            run_id=self.run_id,
            key=key,
            task_id=self.producer_task_id,
            dag_id=self.dag_id,
            map_index=self.map_index,
        )

    async def get_xcom_val_af2(self, key: str) -> Any | None:
        from airflow.models import TaskInstance
        from airflow.utils.session import create_session

        def _get_xcom_val() -> Any | None:
            with create_session() as session:
                ti = (
                    session.query(TaskInstance)
                    .filter_by(
                        dag_id=self.dag_id,
                        task_id=self.producer_task_id,
                        run_id=self.run_id,
                    )
                    .one()
                )
                return ti.xcom_pull(task_ids=self.producer_task_id, key=key)

        return await sync_to_async(_get_xcom_val)()

    async def get_xcom_val(self, key: str) -> Any | None:
        if AIRFLOW_VERSION < Version("3.0.0"):
            return await self.get_xcom_val_af2(key)
        else:
            return await self.get_xcom_val_af3(key)

    async def _parse_node_status(self) -> str | None:
        key = f"nodefinished_{self.model_unique_id.replace('.', '__')}" if self.use_event else "run_results"

        compressed_xcom_val = await self.get_xcom_val(key)
        if not compressed_xcom_val:
            return None

        data_json = _parse_compressed_xcom(compressed_xcom_val)

        if self.use_event:
            return data_json.get("data", {}).get("run_result", {}).get("status")  # type: ignore[no-any-return]

        results = data_json.get("results", [])
        node_result: dict[str, Any] = next(
            (r for r in results if r.get("unique_id") == self.model_unique_id),
            {},
        )
        return node_result.get("status")

    async def _get_producer_task_status(self) -> str | None:
        """Retrieve the producer task state for both Airflow 2 and Airflow 3."""

        if AIRFLOW_VERSION < Version("3.0.0"):
            try:
                from airflow.models import TaskInstance
                from airflow.utils.session import create_session
            except ImportError as exc:  # pragma: no cover - defensive fallback for limited test envs
                self.log.warning("Could not import create_session to read producer state: %s", exc)
                return None

            def _fetch_state() -> str | None:
                with create_session() as session:
                    ti = (
                        session.query(TaskInstance)
                        .filter_by(
                            dag_id=self.dag_id,
                            task_id=self.producer_task_id,
                            run_id=self.run_id,
                        )
                        .first()
                    )
                    if ti is not None:
                        return str(ti.state)
                    return None

            return await sync_to_async(_fetch_state)()

        try:
            from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
        except (ImportError, NameError) as exc:
            self.log.warning("Could not retrieve producer task status via RuntimeTaskInstance: %s", exc)
            return None

        def _fetch_states() -> dict[str, dict[str, Any]]:
            return RuntimeTaskInstance.get_task_states(
                dag_id=self.dag_id,
                task_ids=[self.producer_task_id],
                run_ids=[self.run_id],
            )

        task_states = await sync_to_async(_fetch_states)()
        state = task_states.get(self.run_id, {}).get(self.producer_task_id)
        if state is not None:
            return str(state)
        return None

    async def run(self) -> AsyncIterator[TriggerEvent]:
        self.log.info("Starting WatcherTrigger for model: %s", self.model_unique_id)

        while True:
            producer_task_state = await self._get_producer_task_status()
            node_status = await self._parse_node_status()
            if node_status == "success":
                self.log.info("Model '%s' succeeded", self.model_unique_id)
                yield TriggerEvent({"status": "success"})  # type: ignore[no-untyped-call]
                return
            elif node_status == "failed":
                self.log.warning("Model '%s' failed", self.model_unique_id)
                yield TriggerEvent({"status": "failed", "reason": "model_failed"})  # type: ignore[no-untyped-call]
                return
            elif producer_task_state == "failed":
                self.log.error(
                    "Watcher producer task '%s' failed before delivering results for model '%s'",
                    self.producer_task_id,
                    self.model_unique_id,
                )
                yield TriggerEvent({"status": "failed", "reason": "producer_failed"})  # type: ignore[no-untyped-call]
                return

            # Sleep briefly before re-polling
            await asyncio.sleep(self.poke_interval)
            self.log.debug("Polling again for model '%s' status...", self.model_unique_id)


def _parse_compressed_xcom(compressed_b64_event_msg: str) -> Any:
    """Decode and decompress a base64-encoded, zlib-compressed XCom payload."""
    compressed_bytes = base64.b64decode(compressed_b64_event_msg)
    event_json_str = zlib.decompress(compressed_bytes).decode("utf-8")
    return json.loads(event_json_str)
