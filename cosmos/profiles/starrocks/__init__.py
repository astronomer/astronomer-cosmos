"""StarRocks Airflow connection -> dbt profile mappings"""

from .user_pass import StarrocksUserPasswordProfileMapping

__all__ = ["StarrocksUserPasswordProfileMapping"]
