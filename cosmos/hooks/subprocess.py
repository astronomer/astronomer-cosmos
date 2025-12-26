# This hook has been refined from the Airflow SubprocessHook, offering an added functionality to the original.
# It presents an alternative option to return the complete command output, as opposed to solely the last line from
# stdout or stderr. This option proves to be highly beneficial for any text analysis that depends on the stdout or
# stderr output of a dbt command.
from __future__ import annotations

import contextlib
import os
import signal
import types
from collections.abc import Callable
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir
from typing import Any, NamedTuple

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

    def run_command(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        output_encoding: str = "utf-8",
        cwd: str | None = None,
        process_log_line: Callable[[str, Any], None] | None = None,
        **kwargs: Any,
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
                if process_log_line:
                    # Ensure we call the function with exactly 2 arguments
                    # If process_log_line is a bound method, extract the underlying function
                    # to avoid passing 'self' as the first argument
                    if isinstance(process_log_line, types.MethodType):
                        # It's a bound method, call the underlying function directly
                        process_log_line.__func__(line, kwargs)
                    else:
                        # It's a regular function or callable, call it directly
                        process_log_line(line, kwargs)

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
