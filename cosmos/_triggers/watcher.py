from __future__ import annotations

import asyncio
import base64
import json
import zlib
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent


class WatcherTrigger(BaseTrigger):
    """
    Trigger that monitors dbt model execution by polling XComs for run status.
    Designed to be deferred by DbtConsumerWatcherSensor.
    """

    def __init__(
        self,
        model_unique_id: str,
        producer_task_id: str,
        dag_id: str,
        run_id: str,
        map_index: int | None,
        use_event: bool,
    ):
        self.model_unique_id = model_unique_id
        self.producer_task_id = producer_task_id
        self.dag_id = dag_id
        self.run_id = run_id
        self.map_index = map_index
        self.use_event = use_event

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger arguments for persistence in the metadata DB."""
        return (
            "cosmos._triggers.watcher.WatcherTrigger",
            {
                "model_unique_id": self.model_unique_id,
                "producer_task_id": self.producer_task_id,
                "dag_id": self.dag_id,
                "run_id": self.run_id,
                "map_index": self.map_index,
                "use_event": self.use_event,
            },
        )

    async def get_xcom_val(self, key: str) -> Any | None:
        """
        Safely fetch an XCom value asynchronously using Airflow SDK’s XCom API.
        This wraps the synchronous XCom.get_one() in an async call.
        """
        from airflow.sdk.execution_time.xcom import XCom
        from asgiref.sync import sync_to_async

        return await sync_to_async(XCom.get_one)(
            run_id=self.run_id,
            key=key,
            task_id=self.producer_task_id,
            dag_id=self.dag_id,
            map_index=self.map_index,
        )

    async def _parse_node_status(self) -> str | None:
        """Parse and return node status from the appropriate XCom key."""
        dr_state = await self.get_xcom_val("state")

        if self.use_event:
            key = f"nodefinished_{self.model_unique_id.replace('.', '__')}"
            compressed_xcom_val = await self.get_xcom_val(key)
            if not compressed_xcom_val:
                return None
            event_json = _parse_compressed_xcom(compressed_xcom_val)
            return event_json.get("data", {}).get("run_result", {}).get("status")  # type: ignore[no-any-return]

        compressed_xcom_val = await self.get_xcom_val("run_results")
        if not compressed_xcom_val:
            return None
        run_results_json = _parse_compressed_xcom(compressed_xcom_val)
        results = run_results_json.get("results", [])
        node_result: dict[str, Any] = next((r for r in results if r.get("unique_id") == self.model_unique_id), {})
        return node_result.get("status") or ("failed" if dr_state == "failed" else None)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Continuously poll XComs until the dbt node finishes or fails."""
        self.log.info(
            "Starting WatcherTrigger for model '%s' (use_event=%s)",
            self.model_unique_id,
            self.use_event,
        )

        while True:
            node_status = await self._parse_node_status()
            if node_status == "success":
                self.log.info("Model '%s' succeeded", self.model_unique_id)
                yield TriggerEvent({"status": "success"})  # type: ignore[no-untyped-call]
                return
            elif node_status == "failed":
                self.log.warning("Model '%s' failed", self.model_unique_id)
                yield TriggerEvent({"status": "failed"})  # type: ignore[no-untyped-call]
                return

            # Sleep briefly before re-polling
            await asyncio.sleep(2)
            self.log.debug("Polling again for model '%s' status...", self.model_unique_id)


def _parse_compressed_xcom(compressed_b64_event_msg: str) -> Any:
    """Decode and decompress a base64-encoded, zlib-compressed XCom payload."""
    compressed_bytes = base64.b64decode(compressed_b64_event_msg)
    event_json_str = zlib.decompress(compressed_bytes).decode("utf-8")
    return json.loads(event_json_str)
