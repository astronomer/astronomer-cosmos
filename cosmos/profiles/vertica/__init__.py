"""Vertica Airflow connection -> dbt profile mappings"""

from .user_pass import VerticaUserPasswordProfileMapping

__all__ = ["VerticaUserPasswordProfileMapping"]
