from __future__ import annotations
import logging

from airflow.utils.log.colored_log import CustomTTYColoredFormatter


LOG_FORMAT: str = (
    "[%(blue)s%(asctime)s%(reset)s] "
    "{%(blue)s%(filename)s:%(reset)s%(lineno)d} "
    "%(log_color)s%(levelname)s%(reset)s - "
    "%(purple)s(astronomer-cosmos)%(reset)s - "
    "%(log_color)s%(message)s%(reset)s"
)


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Get custom Astronomer cosmos logger.

    Airflow logs usually look like:
    [2023-08-09T14:20:55.532+0100] {subprocess.py:94} INFO - 13:20:55  Completed successfully

    By using this logger, we introduce a (yellow) astronomer-cosmos string into the project's log messages:
    [2023-08-09T14:20:55.532+0100] {subprocess.py:94} INFO - (astronomer-cosmos) - 13:20:55  Completed successfully
    """
    logger = logging.getLogger(name)
    formatter: logging.Formatter = CustomTTYColoredFormatter(fmt=LOG_FORMAT)  # type: ignore
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
