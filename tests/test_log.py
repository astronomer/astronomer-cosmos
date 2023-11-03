import logging

from cosmos.log import get_logger
from airflow.configuration import conf


def test_get_logger():
    custom_string = "%(purple)s(astronomer-cosmos)%(reset)s"
    standard_logger = logging.getLogger()
    assert custom_string not in standard_logger.handlers[0].formatter._fmt

    custom_logger = get_logger("cosmos-log")
    assert custom_logger.propagate is True
    assert custom_logger.handlers[0].formatter.__class__.__name__ == "CustomTTYColoredFormatter"
    assert custom_string in custom_logger.handlers[0].formatter._fmt

def test_propagate_logs_conf():
    if not conf.has_section("cosmos"):
        conf.add_section("cosmos")
    conf.set("cosmos","propagate_logs","False")
    custom_logger = get_logger("cosmos-log")
    assert custom_logger.propagate is False
