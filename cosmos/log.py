from __future__ import annotations

import logging


class CosmosRichLogger(logging.Logger):
    """Custom Logger that prepends ``(astronomer-cosmos)`` to each log message in the scheduler."""

    def handle(self, record: logging.LogRecord) -> None:
        record.msg = "\x1b[35m(astronomer-cosmos)\x1b[0m " + record.msg
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
    # Use getattr to tolerate cosmos.settings being partially initialized during
    # circular imports (e.g. Airflow plugin discovery in Astro Runtime).
    try:
        import cosmos.settings as _settings

        _rich = getattr(_settings, "rich_logging", False)
    except ImportError:
        _rich = False

    if _rich:
        cls = logging.getLoggerClass()
        try:
            logging.setLoggerClass(CosmosRichLogger)
            return logging.getLogger(name)
        finally:
            logging.setLoggerClass(cls)
    else:
        return logging.getLogger(name)
