import shutil


def get_system_dbt():
    return shutil.which("dbt-ol") or shutil.which("dbt") or "dbt"
