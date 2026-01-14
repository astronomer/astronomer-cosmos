import shutil


def get_system_dbt() -> str:
    """
    Tries to identify which is the path to the dbt executable, return "dbt" otherwise.
    """
    return shutil.which("dbt") or "dbt"


def is_dbt_installed_in_same_environment() -> bool:
    """
    Checks if dbt is installed in the same environment as the current one.
    """
    try:
        import dbt  # noqa: F401
    except ImportError:
        return False
    else:
        return True
