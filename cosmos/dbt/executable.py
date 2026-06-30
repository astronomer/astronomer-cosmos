from __future__ import annotations

import shutil
from functools import cache
from importlib.metadata import PackageNotFoundError, version
from importlib.util import find_spec

from packaging.version import Version


def get_system_dbt() -> str:
    """
    Tries to identify which is the path to the dbt executable, return "dbt" otherwise.
    """
    return shutil.which("dbt") or "dbt"


@cache
def get_dbt_version() -> Version | None:
    """
    Return the installed dbt-core version, or None if dbt-core is not installed.

    Reads the version from the installed package metadata rather than the ``dbt.version``
    module: dbt-core 2.0 is CLI-only and no longer ships an importable ``dbt`` package, so
    ``from dbt.version import __version__`` raises there. Package metadata works for both
    dbt 1.x and 2.x. Cached for the process lifetime since the installed version is fixed.
    """
    try:
        return Version(version("dbt-core"))
    except PackageNotFoundError:
        return None


def is_dbt_installed_in_same_environment() -> bool:
    """
    Checks if dbt is installed in the same environment as the current one.
    """
    try:
        find_spec("dbt")
    except ImportError:
        return False
    else:
        return True
