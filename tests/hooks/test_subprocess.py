from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch
import signal

import pytest

from cosmos.hooks.subprocess import FullOutputSubprocessHook

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
