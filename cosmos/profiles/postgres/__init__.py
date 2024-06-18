"""Postgres Airflow connection -> dbt profile mappings"""

from .user_pass import PostgresUserPasswordProfileMapping

__all__ = ["PostgresUserPasswordProfileMapping"]
