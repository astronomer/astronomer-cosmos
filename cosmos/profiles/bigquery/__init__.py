"""BigQuery Airflow connection -> dbt profile mappings"""

from .oauth import GoogleCloudOauthProfileMapping
from .service_account_file import GoogleCloudServiceAccountFileProfileMapping
from .service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping

__all__ = [
    "GoogleCloudServiceAccountFileProfileMapping",
    "GoogleCloudServiceAccountDictProfileMapping",
    "GoogleCloudOauthProfileMapping",
]
