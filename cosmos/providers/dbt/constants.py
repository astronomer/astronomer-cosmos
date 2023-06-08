import os
from pathlib import Path

DBT_PROFILE_PATH = Path(os.path.expanduser("~")).joinpath(".dbt/profiles.yml")
DEFAULT_DBT_PROFILE_NAME = "cosmos_profile"
