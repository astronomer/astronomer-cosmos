import math
import time
from collections.abc import Callable
from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes import __version__ as airflow_k8s_provider_version
from airflow.providers.cncf.kubernetes.callbacks import ExecutionMode
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodLoggingStatus, PodManager
from airflow.utils.timezone import utcnow
from kubernetes.client.models.v1_pod import V1Pod
from packaging.version import Version
from pendulum import DateTime
from urllib3.exceptions import HTTPError, TimeoutError


# This is being added to overcome the issue with the KubernetesPodOperator logs repeating:
# https://github.com/apache/airflow/issues/59366
# It can be removed once it is fixed in the upstream provider.
class CosmosKubernetesPodManager(PodManager):  # type: ignore[misc]
    """Create, monitor, and otherwise interact with Kubernetes pods for use with the KubernetesPodOperator."""

    def fetch_container_logs(  # noqa: C901
        self,
        pod: V1Pod,
        container_name: str,
        *,
        follow: bool = False,
        since_time: DateTime | None = None,
        post_termination_timeout: int = 120,
        container_name_log_prefix_enabled: bool = True,
        log_formatter: Callable[[str, str], str] | None = None,
    ) -> PodLoggingStatus:
        """
        Follow the logs of container and stream to airflow logging.

        Returns when container exits.

        Between when the pod starts and logs being available, there might be a delay due to CSR not approved
        and signed yet. In such situation, ApiException is thrown. This is why we are retrying on this
        specific exception.

        :meta private:
        """

        def consume_logs(  # noqa: C901
            *, since_time: DateTime | None = None
        ) -> tuple[DateTime | None, Exception | None]:
            """
            Try to follow container logs until container completes.

            For a long-running container, sometimes the log read may be interrupted
            Such errors of this kind are suppressed.

            Returns the last timestamp observed in logs.
            """

            # CUSTOM: Introduced these four lines, modifying the 1.11.0 K8s provider code
            if Version(airflow_k8s_provider_version) >= Version("1.10.0"):
                from airflow.providers.cncf.kubernetes.utils.pod_manager import parse_log_line
            else:
                parse_log_line = self.parse_log_line

            exception = None
            last_captured_timestamp = None
            # We timeout connections after 30 minutes because otherwise they can get
            # stuck forever. The 30 is somewhat arbitrary.
            # As a consequence, a TimeoutError will be raised no more than 30 minutes
            # after starting read.
            connection_timeout = 60 * 30
            # We set a shorter read timeout because that helps reduce *connection* timeouts
            # (since the connection will be restarted periodically). And with read timeout,
            # we don't need to worry about either duplicate messages or losing messages; we
            # can safely resume from a few seconds later
            read_timeout = 60 * 5
            try:
                since_seconds = None
                if since_time:
                    try:
                        since_seconds = math.ceil((pendulum.now() - since_time).total_seconds())
                    except TypeError:
                        self.log.warning(
                            "Error calculating since_seconds with since_time %s. Using None instead.",
                            since_time,
                        )
                logs = self.read_pod_logs(
                    pod=pod,
                    container_name=container_name,
                    timestamps=True,
                    since_seconds=since_seconds,
                    follow=follow,
                    post_termination_timeout=post_termination_timeout,
                    _request_timeout=(connection_timeout, read_timeout),
                )
                message_to_log = None
                message_timestamp = None
                progress_callback_lines = []
                try:
                    for raw_line in logs:
                        line = raw_line.decode("utf-8", errors="backslashreplace")
                        line_timestamp, message = parse_log_line(line)
                        if line_timestamp:  # detect new log line
                            if message_to_log is None:  # first line in the log
                                message_to_log = message
                                message_timestamp = line_timestamp
                                progress_callback_lines.append(line)
                            else:  # previous log line is complete
                                if message_to_log is not None:
                                    self._log_message(
                                        message_to_log,
                                        container_name,
                                        container_name_log_prefix_enabled,
                                        log_formatter,
                                    )
                                    # CUSTOM: Change where callbacks are invoked from
                                    for callback in self._callbacks:
                                        callback.progress_callback(
                                            line=line, client=self._client, mode=ExecutionMode.SYNC
                                        )
                                last_captured_timestamp = message_timestamp
                                message_to_log = message
                                message_timestamp = line_timestamp
                        else:  # continuation of the previous log line
                            message_to_log = f"{message_to_log}\n{message}"
                            progress_callback_lines.append(line)
                finally:
                    # CUSTOM: Change where callbacks are invoked from
                    for callback in self._callbacks:
                        callback.progress_callback(line=message_to_log, client=self._client, mode=ExecutionMode.SYNC)
                    # log the last line and update the last_captured_timestamp
                    if message_to_log is not None:
                        self._log_message(
                            message_to_log, container_name, container_name_log_prefix_enabled, log_formatter
                        )
                    last_captured_timestamp = message_timestamp
            except TimeoutError as e:
                # in case of timeout, increment return time by 2 seconds to avoid
                # duplicate log entries
                if val := (last_captured_timestamp or since_time):
                    return val.add(seconds=2), e
            except HTTPError as e:
                exception = e
                self._http_error_timestamps = getattr(self, "_http_error_timestamps", [])
                self._http_error_timestamps = [
                    t for t in self._http_error_timestamps if t > utcnow() - timedelta(seconds=60)
                ]
                self._http_error_timestamps.append(utcnow())
                # Log only if more than 2 errors occurred in the last 60 seconds
                if len(self._http_error_timestamps) > 2:
                    self.log.exception(
                        "Reading of logs interrupted for container %r; will retry.",
                        container_name,
                    )
            return last_captured_timestamp or since_time, exception

        # note: `read_pod_logs` follows the logs, so we shouldn't necessarily *need* to
        # loop as we do here. But in a long-running process we might temporarily lose connectivity.
        # So the looping logic is there to let us resume following the logs.
        last_log_time = since_time
        while True:
            last_log_time, exc = consume_logs(since_time=last_log_time)
            if not self.container_is_running(pod, container_name=container_name):
                return PodLoggingStatus(running=False, last_log_time=last_log_time)
            if not follow:
                return PodLoggingStatus(running=True, last_log_time=last_log_time)
            # a timeout is a normal thing and we ignore it and resume following logs
            if not isinstance(exc, TimeoutError):
                self.log.warning(
                    "Pod %s log read interrupted but container %s still running. Logs generated in the last one second might get duplicated.",
                    pod.metadata.name,
                    container_name,
                )
            time.sleep(1)
