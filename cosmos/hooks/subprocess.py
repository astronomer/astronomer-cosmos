# This hook has been refined from the Airflow SubprocessHook, offering an added functionality to the original.
# It presents an alternative option to return the complete command output, as opposed to solely the last line from
# stdout or stderr. This option proves to be highly beneficial for any text analysis that depends on the stdout or
# stderr output of a dbt command.
from __future__ import annotations

import contextlib
import json
import os
import signal
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir
from typing import TYPE_CHECKING, NamedTuple

from cosmos._utils.common import safe_xcom_push

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    # Airflow 3.1 onwards
    from airflow.sdk.bases.hook import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook


class FullOutputSubprocessResult(NamedTuple):
    exit_code: int
    output: str
    full_output: list[str]


class FullOutputSubprocessHook(BaseHook):  # type: ignore[misc]
    """Hook for running processes with the ``subprocess`` module."""

    def __init__(self) -> None:
        self.sub_process: Popen[str] | None = None
        super().__init__()  # type: ignore[no-untyped-call]

    def _store_dbt_resource_status_from_log(self, line: str, context: Context | None = None) -> None:
        assert context is not None  # Make MyPy happy
        try:
            log_line = json.loads(line)
            node_status = log_line.get("data", {}).get("node_info", {}).get("node_status")
            unique_id = log_line.get("data", {}).get("node_info", {}).get("unique_id")

            self.log.debug("Model: %s is in %s state", unique_id, node_status)

            if node_status in ["success" or "failed"]:
                safe_xcom_push(
                    task_instance=context["ti"], key=f"{unique_id.replace('.', '__')}_status", value=node_status
                )
        except json.JSONDecodeError:
            self.log.debug("Failed to parse log: %s", line)

    def run_command(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        output_encoding: str = "utf-8",
        cwd: str | None = None,
        context: Context | None = None,
    ) -> FullOutputSubprocessResult:
        """
        Execute the command.

        If ``cwd`` is None, execute the command in a temporary directory which will be cleaned afterwards.
        If ``env`` is not supplied, ``os.environ`` is passed

        :param command: the command to run
        :param env: Optional dict containing environment variables to be made available to the shell
            environment in which ``command`` will be executed.  If omitted, ``os.environ`` will be used.
            Note, that in case you have Sentry configured, original variables from the environment
            will also be passed to the subprocess with ``SUBPROCESS_`` prefix.
        :param output_encoding: encoding to use for decoding stdout
        :param cwd: Working directory to run the command in.
            If None (default), the command is run in a temporary directory.
        :return: :class:`namedtuple` containing:
                                    ``exit_code``
                                    ``output``: the last line from stderr or stdout
                                    ``full_output``: all lines from stderr or stdout.
        """
        self.log.info("Tmp dir root location: \n %s", gettempdir())
        log_lines = []
        with contextlib.ExitStack() as stack:
            if cwd is None:
                cwd = stack.enter_context(TemporaryDirectory(prefix="airflowtmp"))

            def pre_exec() -> None:
                # Restore default signal disposition and invoke setsid
                for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info("Running command: %s", command)

            self.sub_process = Popen(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
                bufsize=1,  # line-buffered (works only in text mode)
                text=True,
                encoding=output_encoding,
                errors="backslashreplace",
            )

            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")

            self.log.info("Command output:")

            last_line: str = ""
            assert self.sub_process.stdout is not None
            for line in self.sub_process.stdout:
                line = line.rstrip("\n")
                last_line = line
                log_lines.append(line)
                self.log.info("%s", line)
                self._store_dbt_resource_status_from_log(line, context)

            # Wait until process completes
            return_code = self.sub_process.wait()

            self.log.info("Command exited with return code %s", return_code)

        return FullOutputSubprocessResult(exit_code=return_code, output=last_line, full_output=log_lines)

    def send_sigterm(self) -> None:
        """Sends SIGTERM signal to ``self.sub_process`` if one exists."""
        self.log.info("Sending SIGTERM signal to process group")
        if self.sub_process and hasattr(self.sub_process, "pid"):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)

    def send_sigint(self) -> None:
        """Sends SIGINT signal to ``self.sub_process`` if one exists."""
        self.log.info("Sending SIGINT signal to process group")
        if self.sub_process and hasattr(self.sub_process, "pid"):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGINT)
