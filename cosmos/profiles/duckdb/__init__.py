"""Duckdb Airflow connection -> dbt profile mappings"""

from .user_pass import DuckDBUserPasswordProfileMapping

__all__ = ["DuckDBUserPasswordProfileMapping"]
