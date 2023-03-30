from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Optional, Tuple

from cosmos.providers.dbt.core.profiles.bigquery import (
    bigquery_profile,
    create_profile_vars_google_cloud_platform,
)
from cosmos.providers.dbt.core.profiles.databricks import (
    create_profile_vars_databricks,
    databricks_profile,
)
from cosmos.providers.dbt.core.profiles.postgres import (
    create_profile_vars_postgres,
    postgres_profile,
)
from cosmos.providers.dbt.core.profiles.redshift import (
    create_profile_vars_redshift,
    redshift_profile,
)
from cosmos.providers.dbt.core.profiles.snowflake import (
    create_profile_vars_snowflake,
    snowflake_profile,
)

if TYPE_CHECKING:
    from airflow.models import Connection


@dataclass
class AdapterConfig:
    profile_name: str
    profile: Dict[str, str]
    create_profile_function: Callable[
        ["Connection", Optional[str], Optional[str]], Tuple[str, Dict[str, str]]
    ]


def get_available_adapters() -> Dict[str, AdapterConfig]:
    return {
        "postgres": AdapterConfig(
            "postgres_profile", postgres_profile, create_profile_vars_postgres
        ),
        "redshift": AdapterConfig(
            "redshift_profile", redshift_profile, create_profile_vars_redshift
        ),
        "snowflake": AdapterConfig(
            "snowflake_profile", snowflake_profile, create_profile_vars_snowflake
        ),
        "google_cloud_platform": AdapterConfig(
            "bigquery_profile",
            bigquery_profile,
            create_profile_vars_google_cloud_platform,
        ),
        "databricks": AdapterConfig(
            "databricks_profile", databricks_profile, create_profile_vars_databricks
        ),
    }
