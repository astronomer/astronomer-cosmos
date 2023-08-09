import logging

from cosmos.log import get_logger


def test_get_logger():
    custom_string = "%(yellow)s(astronomer-cosmos)%(reset)s"
    standard_logger = logging.getLogger()
    assert custom_string not in standard_logger.handlers[0].formatter._fmt

    custom_logger = get_logger("cosmos-log")
    assert custom_logger.handlers[0].formatter.__class__.__name__ == "CustomTTYColoredFormatter"
    assert custom_string in custom_logger.handlers[0].formatter._fmt
