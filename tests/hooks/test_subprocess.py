import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest

from cosmos.hooks.subprocess import FullOutputSubprocessHook
from cosmos.operators.watcher import store_dbt_resource_status_from_log

OS_ENV_KEY = "SUBPROCESS_ENV_TEST"
OS_ENV_VAL = "this-is-from-os-environ"


@pytest.mark.parametrize(
    "env,expected",
    [
        ({"ABC": "123", "AAA": "456"}, {"ABC": "123", "AAA": "456", OS_ENV_KEY: ""}),
        ({}, {OS_ENV_KEY: ""}),
        (None, {OS_ENV_KEY: OS_ENV_VAL}),
    ],
    ids=["with env", "empty env", "no env"],
)
def test_env(env, expected):
    """
    Test that env variables are exported correctly to the command environment.
    When ``env`` is ``None``, ``os.environ`` should be passed to ``Popen``.
    Otherwise, the variables in ``env`` should be available, and ``os.environ`` should not.
    """
    hook = FullOutputSubprocessHook()

    def build_cmd(keys, filename):
        """
        Produce bash command to echo env vars into filename.
        Will always echo the special test var named ``OS_ENV_KEY`` into the file to test whether
        ``os.environ`` is passed or not.
        """
        return "\n".join(f"echo {k}=${k}>> {filename}" for k in [*keys, OS_ENV_KEY])

    with TemporaryDirectory() as tmp_dir, patch.dict("os.environ", {OS_ENV_KEY: OS_ENV_VAL}):
        tmp_file = Path(tmp_dir, "test.txt")
        command = build_cmd(env and env.keys() or [], tmp_file.as_posix())
        hook.run_command(command=["bash", "-c", command], env=env)
        actual = dict([x.split("=") for x in tmp_file.read_text().splitlines()])
        assert actual == expected


def test_subprocess_hook():
    hook = FullOutputSubprocessHook()
    result = hook.run_command(command=["bash", "-c", f'echo "foo"'])
    assert result.exit_code == 0
    assert result.output == "foo"
    assert result.full_output == ["foo"]


def test_run_command_runtime_error():
    hook = FullOutputSubprocessHook()

    with patch("cosmos.hooks.subprocess.Popen", return_value=None):
        with pytest.raises(RuntimeError, match="The subprocess should be created here and is None!"):
            hook.run_command(["echo", "hello"])


@patch("os.getpgid", return_value=123)
@patch("os.killpg")
def test_send_sigint(mock_killpg, mock_getpgid):
    hook = FullOutputSubprocessHook()
    hook.sub_process = MagicMock()
    hook.send_sigint()
    mock_killpg.assert_called_with(123, signal.SIGINT)


@patch("os.getpgid", return_value=123)
@patch("os.killpg")
def test_send_sigterm(mock_killpg, mock_getpgid):
    hook = FullOutputSubprocessHook()
    hook.sub_process = MagicMock()
    hook.send_sigterm()
    mock_killpg.assert_called_with(123, signal.SIGTERM)


FORK_HOOK_PROBE = """
import os
import sys

from cosmos.hooks.subprocess import FullOutputSubprocessHook

fork_hook_calls = []
os.register_at_fork(before=lambda: fork_hook_calls.append("before"))
FullOutputSubprocessHook().run_command([sys.executable, "-c", "pass"])
print(f"fork_hook_calls={len(fork_hook_calls)}")
"""


def test_run_command_does_not_run_fork_hooks():
    # gRPC and other C extensions with fork handlers can hang or abort the task when they run.
    probe = subprocess.run([sys.executable, "-c", FORK_HOOK_PROBE], capture_output=True, text=True, check=True)

    assert "fork_hook_calls=0" in probe.stdout.splitlines()


def test_run_command_starts_the_command_in_its_own_session():
    result = FullOutputSubprocessHook().run_command(
        [sys.executable, "-c", "import os; print(os.getpid(), os.getpgid(0), os.getsid(0))"]
    )

    pid, process_group_id, session_id = map(int, result.output.split())
    assert pid == process_group_id == session_id
    assert session_id != os.getsid(0)


@pytest.mark.parametrize(
    "status,context,expect_xcom_push",
    [
        ("success", {"ti": MagicMock()}, True),
        ("failed", {"ti": MagicMock()}, True),
        ("success", None, False),
        ("failed", None, False),
    ],
)
def test_store_dbt_resource_status_from_log_param(status, context, expect_xcom_push):
    # Prepare log line
    log_line = {"data": {"node_info": {"node_status": status, "unique_id": "model.jaffle_shop.stg_orders"}}}
    line = json.dumps(log_line)

    with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
        store_dbt_resource_status_from_log(line, {"context": context}, tests_per_model={}, test_results_per_model={})
        if expect_xcom_push:
            mock_push.assert_called_with(
                task_instance=context["ti"],
                key="model__jaffle_shop__stg_orders_status",
                value={"status": status, "outlet_uris": []},
            )
            assert mock_push.call_count == 1
        else:
            mock_push.assert_not_called()


def test_store_dbt_resource_status_from_log_invalid_json():
    invalid_line = "{not a valid json}"

    with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
        store_dbt_resource_status_from_log(
            invalid_line, {"context": {"ti": MagicMock()}}, tests_per_model={}, test_results_per_model={}
        )
        mock_push.assert_not_called()
