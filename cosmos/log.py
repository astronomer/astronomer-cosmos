from __future__ import annotations

import logging


class CosmosRichLogger(logging.Logger):
    """Custom Logger that prepends ``(astronomer-cosmos)`` to each log message in the scheduler."""

    def handle(self, record: logging.LogRecord) -> None:
        if record.msg is not None:
            record.msg = "\x1b[35m(astronomer-cosmos)\x1b[0m " + str(record.msg)
        return super().handle(record)


def get_logger(name: str) -> logging.Logger:
    """
    Get custom Astronomer cosmos logger.

    Airflow logs usually look like:
    [2023-08-09T14:20:55.532+0100] {subprocess.py:94} INFO - 13:20:55  Completed successfully

    This logger introduces a (magenta) astronomer-cosmos string into the project's log messages,
    as long as the ``rich_logging`` setting is True:
    [2023-08-09T14:20:55.532+0100] {subprocess.py:94} INFO - (astronomer-cosmos) - 13:20:55  Completed successfully
    """
    # Import cosmos.settings at call time (not module level) to avoid circular imports
    # during Airflow plugin discovery. getattr tolerates the module being partially
    # initialized (rich_logging not yet defined) by falling back to False.
    import cosmos.settings as _settings

    _rich = getattr(_settings, "rich_logging", False)

    if _rich:
        cls = logging.getLoggerClass()
        try:
            logging.setLoggerClass(CosmosRichLogger)
            return logging.getLogger(name)
        finally:
            logging.setLoggerClass(cls)
    else:
        return logging.getLogger(name)
