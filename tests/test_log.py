import logging

import pytest

import cosmos.log
from cosmos import get_provider_info
from cosmos.log import get_logger
from cosmos.log import CosmosRichLogger


def test_get_logger(monkeypatch):
    monkeypatch.setattr(cosmos.log, "rich_logging", False)
    standard_logger = get_logger("cosmos")
    assert not isinstance(standard_logger, CosmosRichLogger)

    monkeypatch.setattr(cosmos.log, "rich_logging", True)
    custom_logger = get_logger("cosmos")
    assert isinstance(custom_logger, CosmosRichLogger)

    with pytest.raises(TypeError):
        # Ensure that the get_logger signature is not changed in the future
        # and name is still a required parameter
        bad_logger = get_logger()  # noqa


def test_rich_logging(monkeypatch, capsys):
    monkeypatch.setattr(cosmos.log, "rich_logging", False)
    standard_logger = get_logger("cosmos")
    standard_logger.info("Hello, world!")
    assert capsys.readouterr() == "Hello, world!\n"

    monkeypatch.setattr(cosmos.log, "rich_logging", True)
    custom_logger = get_logger("cosmos")
    custom_logger.info("Hello, world!")
    assert capsys.readouterr() == "\x1b[35m(astronomer-cosmos)\x1b[0m Hello, world!\n"


def test_get_provider_info():
    provider_info = get_provider_info()
    assert "cosmos" in provider_info.get("config").keys()
    assert "options" in provider_info.get("config").get("cosmos").keys()
    assert "propagate_logs" in provider_info.get("config").get("cosmos").get("options").keys()
    assert provider_info["config"]["cosmos"]["options"]["propagate_logs"]["type"] == "boolean"
