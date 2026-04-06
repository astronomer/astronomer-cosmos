from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from enum import Enum
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async
from packaging.version import Version

from cosmos.constants import _DBT_STARTUP_EVENTS_XCOM_KEY, AIRFLOW_VERSION
from cosmos.listeners.dag_run_listener import EventStatus
from cosmos.log import get_logger
from cosmos.operators._watcher.state import (
    _log_dbt_event,
    build_producer_state_fetcher,
    is_dbt_node_status_failed,
    is_dbt_node_status_skipped,
    is_dbt_node_status_success,
    is_dbt_node_status_terminal,
)

logger = get_logger(__name__)


class WatcherEventReason(str, Enum):
    """Reason codes used in TriggerEvent payloads between WatcherTrigger and BaseConsumerSensor.execute_complete."""

    NODE_FAILED = "node_failed"
    PRODUCER_FAILED = "producer_failed"
    NODE_NOT_RUN = "node_not_run"


class WatcherTrigger(BaseTrigger):

    def __init__(
        self,
        model_unique_id: str,
        producer_task_id: str,
        dag_id: str,
        run_id: str,
        map_index: int | None,
        poke_interval: float = 5.0,
        is_test_sensor: bool = False,
        # Accepted for upgrade-compatibility only: triggers serialized before the
        # invocation-mode unification may still carry this kwarg (Cosmos < 1.14.0). It is no longer
        # used because both SUBPROCESS and DBT_RUNNER now push the same *_status
        # XCom keys, so the trigger does not need to know the invocation mode.
        use_event: bool = True,  # noqa: ARG002
    ):
        self.model_unique_id = model_unique_id
        self.producer_task_id = producer_task_id
        self.dag_id = dag_id
        self.run_id = run_id
        self.map_index = map_index
        self.poke_interval = poke_interval
        self.is_test_sensor = is_test_sensor

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "cosmos.operators._watcher.triggerer.WatcherTrigger",
            {
                "model_unique_id": self.model_unique_id,
                "producer_task_id": self.producer_task_id,
                "dag_id": self.dag_id,
                "run_id": self.run_id,
                "map_index": self.map_index,
                "poke_interval": self.poke_interval,
                "is_test_sensor": self.is_test_sensor,
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

    async def _get_node_status(self) -> Any | None:
        """Return the dbt node status from XCom.

        The XCom value is always a dict with ``status`` and ``outlet_uris`` keys.
        Stores outlet URIs on ``self._outlet_uris`` for later dataset emission.
        """
        status_key = f"{self.model_unique_id.replace('.', '__')}_status"
        xcom_val = await self.get_xcom_val(status_key)
        if xcom_val is None:
            return None
        self._outlet_uris = xcom_val.get("outlet_uris", [])
        return xcom_val.get("status")

    async def _parse_dbt_node_status_and_compiled_sql(self) -> tuple[str | None, str | None]:
        """
        Parse node status and compiled_sql from XCom.

        Returns a tuple of (status, compiled_sql).

        For test sensors (``is_test_sensor=True``), the aggregated test status is
        read from the ``<model_uid>_tests_status`` key. No compiled_sql is relevant.

        For regular sensors, status is read from the per-model ``*_status`` XCom key
        pushed by store_dbt_resource_status_from_log (same key for both SUBPROCESS
        and DBT_RUNNER invocation modes).
        compiled_sql is always read from the canonical per-model ``*_compiled_sql`` key.
        """
        if self.is_test_sensor:
            from cosmos.operators._watcher.aggregation import get_tests_status_xcom_key

            status_key = get_tests_status_xcom_key(self.model_unique_id)
            status = await self.get_xcom_val(status_key)
            return status, None

        compiled_sql_key = f"{self.model_unique_id.replace('.', '__')}_compiled_sql"

        status = await self._get_node_status()
        compiled_sql = await self.get_xcom_val(compiled_sql_key) if status is not None else None
        return status, compiled_sql

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

    async def _log_startup_events(self) -> None:
        """Wait for dbt_startup_events from producer (pushed early in runner callback; from log in subprocess) and log versions."""
        main_logged = False
        adapter_logged = False

        while True:

            events = await self.get_xcom_val(_DBT_STARTUP_EVENTS_XCOM_KEY)

            if isinstance(events, list) and events:
                # Process the full events list so we log both MainReportVersion and
                # AdapterRegistered when present, then decide whether to return.
                for ev in events:
                    name, msg = ev.get("name"), ev.get("msg") or ""

                    if not main_logged and name == "MainReportVersion":
                        logger.info("%s", msg)
                        main_logged = True
                    elif not adapter_logged and name == "AdapterRegistered":
                        logger.info("%s", msg)
                        adapter_logged = True

                if main_logged and adapter_logged:
                    return

            # Check producer status after processing events so we never return before
            # logging the full list. Also ensures we exit if producer finishes before
            # ever pushing _DBT_STARTUP_EVENTS_XCOM_KEY.
            producer_task_state = await self._get_producer_task_status()
            if producer_task_state in ("failed", "success"):
                return

            # Return if dbt node is in terminal state
            status = await self._get_node_status()
            if is_dbt_node_status_terminal(status):
                return

            await asyncio.sleep(self.poke_interval)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        logger.info("Starting WatcherTrigger for model: %s", self.model_unique_id)
        await self._log_startup_events()

        while True:
            producer_task_state = await self._get_producer_task_status()
            dbt_log_event = await self.get_xcom_val(f"{self.model_unique_id.replace('.', '__')}_dbt_event")
            _log_dbt_event(dbt_log_event)
            dbt_node_status, compiled_sql = await self._parse_dbt_node_status_and_compiled_sql()
            if is_dbt_node_status_success(dbt_node_status):
                logger.debug("dbt node '%s' succeeded", self.model_unique_id)
                event_data: dict[str, Any] = {"status": EventStatus.SUCCESS}
                if compiled_sql:
                    event_data["compiled_sql"] = compiled_sql
                # Pass outlet URIs through TriggerEvent so consumer can emit datasets
                outlet_uris = getattr(self, "_outlet_uris", [])
                if outlet_uris:
                    event_data["outlet_uris"] = outlet_uris
                yield TriggerEvent(event_data)  # type: ignore[no-untyped-call]
                return
            elif is_dbt_node_status_skipped(dbt_node_status):
                logger.info("dbt node '%s' was skipped", self.model_unique_id)
                yield TriggerEvent({"status": EventStatus.SKIPPED})  # type: ignore[no-untyped-call]
                return
            elif is_dbt_node_status_failed(dbt_node_status):
                logger.warning("dbt node '%s' failed", self.model_unique_id)
                event_data = {"status": EventStatus.FAILED, "reason": WatcherEventReason.NODE_FAILED}
                if compiled_sql:
                    event_data["compiled_sql"] = compiled_sql
                yield TriggerEvent(event_data)  # type: ignore[no-untyped-call]
                return
            elif producer_task_state == "failed":
                logger.error(
                    "Watcher producer task '%s' failed before delivering results for node '%s'",
                    self.producer_task_id,
                    self.model_unique_id,
                )
                yield TriggerEvent({"status": EventStatus.FAILED, "reason": WatcherEventReason.PRODUCER_FAILED})  # type: ignore[no-untyped-call]
                return
            elif producer_task_state == "success" and dbt_node_status is None:
                logger.info(
                    "The producer task '%s' succeeded. There is no information about the node '%s' execution.",
                    self.producer_task_id,
                    self.model_unique_id,
                )
                yield TriggerEvent({"status": EventStatus.SUCCESS, "reason": WatcherEventReason.NODE_NOT_RUN})  # type: ignore[no-untyped-call]
                return

            # Sleep briefly before re-polling
            await asyncio.sleep(self.poke_interval)
            logger.debug(
                "Polling again for node '%s': status=%s, producer_state=%s",
                self.model_unique_id,
                dbt_node_status,
                producer_task_state,
            )
