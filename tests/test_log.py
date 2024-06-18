import logging

import pytest

from cosmos import get_provider_info
from cosmos.log import get_logger



def test_get_logger():
    custom_string = "%(purple)s(astronomer-cosmos)%(reset)s"
    standard_logger = logging.getLogger()
    assert custom_string not in standard_logger.handlers[0].formatter._fmt

    custom_logger = get_logger("cosmos-log")
    assert custom_logger.propagate is True
    assert custom_logger.handlers[0].formatter.__class__.__name__ == "CustomTTYColoredFormatter"
    assert custom_string in custom_logger.handlers[0].formatter._fmt

    with pytest.raises(TypeError):
        # Ensure that the get_logger signature is not changed in the future
        # and name is still a required parameter
        custom_logger = get_logger() # noqa

    # Explicitly ensure that even if we pass None or empty string
    # we will not get root logger in any case
    custom_logger = get_logger('')
    assert custom_logger.name != ''

    custom_logger = get_logger(None) # noqa
    assert custom_logger.name != ''

def test_propagate_logs_conf(monkeypatch):
    monkeypatch.setattr("cosmos.log.propagate_logs", False)
    custom_logger = get_logger("cosmos-log")
    assert custom_logger.propagate is False


def test_get_provider_info():
    provider_info = get_provider_info()
    assert "cosmos" in provider_info.get("config").keys()
    assert "options" in provider_info.get("config").get("cosmos").keys()
    assert "propagate_logs" in provider_info.get("config").get("cosmos").get("options").keys()
    assert provider_info["config"]["cosmos"]["options"]["propagate_logs"]["type"] == "boolean"
