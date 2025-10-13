"""mysql Airflow connection -> dbt profile mappings"""

from .user_pass import MysqlUserPasswordProfileMapping

__all__ = ["MysqlUserPasswordProfileMapping"]
