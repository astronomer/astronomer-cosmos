from __future__ import annotations

from urllib.parse import urlencode

import httpx

from cosmos import constants
from cosmos.log import get_logger

logger = get_logger(__name__)


class ScarfTelemetryClient:
    """Thin wrapper for sending telemetry events to the Scarf gateway."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = constants.TELEMETRY_TIMEOUT,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url or constants.TELEMETRY_URL
        self._timeout = timeout
        self._client = http_client

    def post(self, metrics: dict[str, object]) -> bool:
        query_string = urlencode(metrics)
        telemetry_url = self._base_url.format(
            **metrics, telemetry_version=constants.TELEMETRY_VERSION, query_string=query_string
        )
        logger.debug("Telemetry is enabled. Emitting usage metrics to %s: %s", telemetry_url, metrics)

        try:
            if self._client:
                response = self._client.post(telemetry_url, timeout=self._timeout)
            else:
                response = httpx.post(telemetry_url, timeout=self._timeout)
        except httpx.HTTPError as exc:
            logger.warning(
                "Unable to emit usage metrics to %s. An HTTP error occurred: %s", telemetry_url, exc
            )
            return False

        if not response.is_success:
            logger.warning(
                "Unable to emit usage metrics to %s. Status code: %s. Message: %s",
                telemetry_url,
                response.status_code,
                response.text,
            )
            return False

        return True
