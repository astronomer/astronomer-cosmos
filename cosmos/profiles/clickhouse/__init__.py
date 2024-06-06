"""Generic Airflow connection -> dbt profile mappings"""

from .user_pass import ClickhouseUserPasswordProfileMapping

__all__ = ["ClickhouseUserPasswordProfileMapping"]
