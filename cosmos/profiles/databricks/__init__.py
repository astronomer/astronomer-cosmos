"""Databricks Airflow connection -> dbt profile mappings"""

from .oauth import DatabricksOauthProfileMapping
from .token import DatabricksTokenProfileMapping

__all__ = ["DatabricksTokenProfileMapping", "DatabricksOauthProfileMapping"]
