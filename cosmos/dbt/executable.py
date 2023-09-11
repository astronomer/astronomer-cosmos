import shutil


def get_system_dbt() -> str:
    """
    Tries to identify which is the path to the dbt executable, return "dbt" otherwise.
    """
    return shutil.which("dbt") or "dbt"
