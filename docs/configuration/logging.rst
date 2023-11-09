.. _logging:

Logging
====================

Cosmos uses a custom logger implementation so that all log messages are clearly tagged with ``(astronomer-cosmos)``. By default this logger has propagation enabled.

In some environments (for example when running Celery workers) this can cause duplicated log messages to appear in the logs. In this case log propagation can be disabled via airflow configuration using the boolean option ``propagate_logs`` under a ``cosmos`` section.

.. code-block:: cfg

    [cosmos]
    propagate_logs = False

or

.. code-block:: python

    AIRFLOW__COSMOS__PROPAGATE_LOGS = "False"
