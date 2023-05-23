# This hook has been refined from the Airflow SubprocessHook, offering an added functionality to the original.
# It presents an alternative option to return the complete command output, as opposed to solely the last line from
# stdout or stderr. This option proves to be highly beneficial for any text analysis that depends on the stdout or
# stderr output of a dbt command.
from __future__ import annotations

import contextlib
import os
import signal
from collections import namedtuple
from pathlib import Path
from subprocess import PIPE, STDOUT, CalledProcessError, Popen, check_output
from tempfile import TemporaryDirectory, gettempdir

from airflow.hooks.base import BaseHook
from airflow.utils.python_virtualenv import prepare_virtualenv

FullOutputSubprocessResult = namedtuple("FullOutputSubprocessResult", ["exit_code", "output", "full_output"])

PY_INTERPRETER = "python3"


class FullOutputSubprocessHook(BaseHook):
    """Hook for running processes with the ``subprocess`` module."""

    def __init__(self) -> None:
        self.sub_process: Popen[bytes] | None = None
        super().__init__()

    def setup_virtualenv(
        self,
        stack: contextlib.ExitStack,
        py_system_site_packages: bool = False,
        py_requirements: list[str] | None = None,
    ):
        venv_tmp_dir = stack.enter_context(TemporaryDirectory(prefix="cosmos-venv"))

        py_interpreter = prepare_virtualenv(
            venv_directory=venv_tmp_dir,
            python_bin=PY_INTERPRETER,
            system_site_packages=py_system_site_packages,
            requirements=py_requirements,
        )
        dbt_binary = Path(py_interpreter).parent / "dbt"

        try:
            dbt_version = (
                check_output(
                    [
                        py_interpreter,
                        "-c",
                        "from importlib.metadata import version; print(version('dbt-core'))",
                    ]
                )
                .decode()
                .strip()
            )
        except CalledProcessError as e:
            raise RuntimeError(
                "Command '{}' return with error (code {}): {}".format(
                    e.cmd, e.returncode, e.output
                )
            )

        self.log.info("Using DBT version %s available at %s", dbt_version, dbt_binary)
        return dbt_binary

    def run_command(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        output_encoding: str = "utf-8",
        cwd: str | None = None,
        py_system_site_packages: bool = False,
        py_requirements: list[str] | None = None,
    ) -> FullOutputSubprocessResult:
        """
        Execute the command.

        If ``cwd`` is None, execute the command in a temporary directory which will be cleaned afterwards.
        If ``env`` is not supplied, ``os.environ`` is passed

        :param command: the command to run
        :param env: Optional dict containing environment variables to be made available to the shell
            environment in which ``command`` will be executed.  If omitted, ``os.environ`` will be used.
            Note, that in case you have Sentry configured, original variables from the environment
            will also be passed to the subprocess with ``SUBPROCESS_`` prefix. See
            :doc:`/administration-and-deployment/logging-monitoring/errors` for details.
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

            if py_requirements:
                dbt_binary_path = self.setup_virtualenv(
                    stack=stack,
                    py_system_site_packages=py_system_site_packages,
                    py_requirements=py_requirements,
                )
                command[0] = str(dbt_binary_path)

            def pre_exec():
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
            )

            self.log.info("Output:")
            line = ""

            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")

            if self.sub_process.stdout is not None:
                for raw_line in iter(self.sub_process.stdout.readline, b""):
                    line = raw_line.decode(output_encoding, errors="backslashreplace").rstrip()
                    # storing the warn & error lines to be used later
                    log_lines.append(line)
                    self.log.info("%s", line)

            self.sub_process.wait()

            self.log.info(
                "Command exited with return code %s", self.sub_process.returncode
            )

        return FullOutputSubprocessResult(
            exit_code=self.sub_process.returncode, output=line, full_output=log_lines
        )

    def send_sigterm(self):
        """Sends SIGTERM signal to ``self.sub_process`` if one exists."""
        self.log.info("Sending SIGTERM signal to process group")
        if self.sub_process and hasattr(self.sub_process, "pid"):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)
