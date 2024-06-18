"""Redshift Airflow connection -> dbt profile mappings"""

from .user_pass import RedshiftUserPasswordProfileMapping

__all__ = ["RedshiftUserPasswordProfileMapping"]
