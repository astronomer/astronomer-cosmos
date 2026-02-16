from __future__ import annotations

import asyncio
import base64
import zlib
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async
from packaging.version import Version

from cosmos import _json as json
from cosmos.constants import AIRFLOW_VERSION
from cosmos.log import get_logger
from cosmos.operators._watcher.state import build_producer_state_fetcher

logger = get_logger(__name__)


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
            "cosmos.operators._watcher.triggerer.WatcherTrigger",
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
                    .one_or_none()
                )
                if ti is None:
                    return None
                return ti.xcom_pull(task_ids=self.producer_task_id, key=key)

        return await sync_to_async(_get_xcom_val)()

    async def get_xcom_val(self, key: str) -> Any | None:
        logger.info(
            "Trying to retrieve value using XCom key <%s> by task_id <%s>, dag_id <%s>, run_id <%s> and map_index <%s>",
            key,
            self.producer_task_id,
            self.dag_id,
            self.run_id,
            self.map_index,
        )
        if AIRFLOW_VERSION < Version("3.0.0"):
            return await self.get_xcom_val_af2(key)
        else:
            return await self.get_xcom_val_af3(key)

    async def _parse_node_status(self) -> str | None:
        key = (
            f"nodefinished_{self.model_unique_id.replace('.', '__')}"
            if self.use_event
            else f"{self.model_unique_id.replace('.', '__')}_status"
        )

        if self.use_event:
            compressed_xcom_val = await self.get_xcom_val(key)
            if not compressed_xcom_val:
                return None

            data_json = _parse_compressed_xcom(compressed_xcom_val)
            return data_json.get("data", {}).get("run_result", {}).get("status")  # type: ignore[no-any-return]

        return await self.get_xcom_val(key)

    async def _get_producer_task_status(self) -> str | None:
        """Retrieve the producer task state for both Airflow 2 and Airflow 3."""

        fetch_state = build_producer_state_fetcher(
            airflow_version=AIRFLOW_VERSION,
            dag_id=self.dag_id,
            run_id=self.run_id,
            producer_task_id=self.producer_task_id,
            logger=logger,
        )
        if fetch_state is None:
            return None

        return await sync_to_async(fetch_state)()

    async def run(self) -> AsyncIterator[TriggerEvent]:
        logger.info("Starting WatcherTrigger for model: %s", self.model_unique_id)

        while True:
            producer_task_state = await self._get_producer_task_status()
            node_status = await self._parse_node_status()
            if node_status == "success":
                logger.info("Model '%s' succeeded", self.model_unique_id)
                yield TriggerEvent({"status": "success"})  # type: ignore[no-untyped-call]
                return
            elif node_status == "failed":
                logger.warning("Model '%s' failed", self.model_unique_id)
                yield TriggerEvent({"status": "failed", "reason": "model_failed"})  # type: ignore[no-untyped-call]
                return
            elif producer_task_state == "failed":
                logger.error(
                    "Watcher producer task '%s' failed before delivering results for model '%s'",
                    self.producer_task_id,
                    self.model_unique_id,
                )
                yield TriggerEvent({"status": "failed", "reason": "producer_failed"})  # type: ignore[no-untyped-call]
                return
            elif producer_task_state == "success" and node_status is None:
                logger.info(
                    "The producer task '%s' succeeded. There is no information about the model '%s' execution.",
                    self.producer_task_id,
                    self.model_unique_id,
                )
                yield TriggerEvent({"status": "success", "reason": "model_not_run"})  # type: ignore[no-untyped-call]
                return

            # Sleep briefly before re-polling
            await asyncio.sleep(self.poke_interval)
            logger.debug("Polling again for model '%s' status...", self.model_unique_id)


def _parse_compressed_xcom(compressed_b64_event_msg: str) -> Any:
    """Decode and decompress a base64-encoded, zlib-compressed XCom payload."""
    compressed_bytes = base64.b64decode(compressed_b64_event_msg)
    event_json_str = zlib.decompress(compressed_bytes).decode("utf-8")
    return json.loads(event_json_str)
