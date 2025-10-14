import airflow
import pytest
from packaging.version import Version

import cosmos.log
from cosmos.log import CosmosRichLogger, get_logger
from cosmos.provider_info import get_provider_info

AIRFLOW_VERSION = Version(airflow.__version__)


def test_get_logger(monkeypatch):
    monkeypatch.setattr(cosmos.log, "rich_logging", False)
    standard_logger = get_logger("test-get-logger-example1")
    assert not isinstance(standard_logger, CosmosRichLogger)

    monkeypatch.setattr(cosmos.log, "rich_logging", True)
    custom_logger = get_logger("test-get-logger-example2")
    assert isinstance(custom_logger, CosmosRichLogger)

    with pytest.raises(TypeError):
        # Ensure that the get_logger signature is not changed in the future
        # and name is still a required parameter
        bad_logger = get_logger()  # noqa


def test_rich_logging(monkeypatch, caplog):
    monkeypatch.setattr(cosmos.log, "rich_logging", False)
    standard_logger = get_logger("test-rich-logging-example1")

    with caplog.at_level("INFO"):
        standard_logger.info("Hello, world!")
    log_output = caplog.text
    assert "Hello, world!" in log_output
    assert "\x1b[35m(astronomer-cosmos)\x1b[0m " not in log_output
    assert log_output.count("\n") == 1

    caplog.clear()
    monkeypatch.setattr(cosmos.log, "rich_logging", True)
    custom_logger = get_logger("test-rich-logging-example2")
    with caplog.at_level("INFO"):
        custom_logger.info("Hello, world!")
    assert "Hello, world!" in caplog.text
    assert "\x1b[35m(astronomer-cosmos)\x1b[0m " in caplog.messages[0]
    assert caplog.text.count("\n") == 1


def test_get_provider_info():
    provider_info = get_provider_info()
    assert "cosmos" in provider_info.get("config").keys()
    assert "options" in provider_info.get("config").get("cosmos").keys()
    assert "propagate_logs" in provider_info.get("config").get("cosmos").get("options").keys()
    assert provider_info["config"]["cosmos"]["options"]["propagate_logs"]["type"] == "boolean"
