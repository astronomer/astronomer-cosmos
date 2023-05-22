from __future__ import annotations

import logging
from collections import namedtuple

from cosmos.providers.dbt.core.operators.local import (
    DbtDocsLocalOperator,
    DbtLocalBaseOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtTestLocalOperator,
)

logger = logging.getLogger(__name__)

FullOutputSubprocessResult = namedtuple(
    "FullOutputSubprocessResult", ["exit_code", "output", "full_output"]
)


class DbtVirtualenvBaseOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core cli command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.

    :param install_deps: If true, install dependencies before running the command
    :param callback: A callback function called on after a dbt run with a path to the dbt project directory.
    :param py_requirements: Creates a virtual environment with the specified arguments
    :param py_system_site_packages: Whether or not all the Python packages from the Airflow instance, will be accessible
           within the virtual environment (if py_requirements argument is specified).
           Avoid using unless the DBT job requires it.
    """

    def __init__(
        self,
        py_requirements: list[str] | None = None,
        py_system_site_packages: bool = False,
        **kwargs,
    ) -> None:
        self.py_requirements = py_requirements
        self.py_system_site_packages = py_system_site_packages or []
        super().__init__(**kwargs)

    def run_subprocess(self, *args, **kwargs):
        return self.subprocess_hook.run_command(
            *args,
            py_requirements=self.py_requirements,
            py_system_site_packages=self.py_system_site_packages,
            **kwargs,
        )


class DbtLSVirtualenvOperator(DbtVirtualenvBaseOperator, DbtLSLocalOperator):
    """
    Executes a dbt core ls command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.
    """


class DbtSeedVirtualenvOperator(DbtVirtualenvBaseOperator, DbtSeedLocalOperator):
    """
    Executes a dbt core seed command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.
    """


class DbtSnapshotVirtualenvOperator(
    DbtVirtualenvBaseOperator, DbtSnapshotLocalOperator
):
    """
    Executes a dbt core snapshot command within a Python Virtual Environment, that is created before running the DBT
    command and deleted just after.
    """


class DbtRunVirtualenvOperator(DbtVirtualenvBaseOperator, DbtRunLocalOperator):
    """
    Executes a dbt core run command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.
    """


class DbtTestVirtualenvOperator(DbtVirtualenvBaseOperator, DbtTestLocalOperator):
    """
    Executes a dbt core test command within a Python Virtual Environment, that is created before running the DBT command
    and deleted just after.
    """


class DbtRunOperationVirtualenvOperator(
    DbtVirtualenvBaseOperator, DbtRunOperationLocalOperator
):
    """
    Executes a dbt core run-operation command within a Python Virtual Environment, that is created before running the
    DBT command and deleted just after.
    """


class DbtDocsVirtualenvOperator(DbtVirtualenvBaseOperator, DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command within a Python Virtual Environment, that is created before running the DBT
    command and deleted just after.
    """
