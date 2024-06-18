"""Exasol Airflow connection -> dbt profile mappings"""

from .user_pass import ExasolUserPasswordProfileMapping

__all__ = ["ExasolUserPasswordProfileMapping"]
