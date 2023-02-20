import os
from pathlib import Path

DBT_PROFILE_PATH = Path(os.path.expanduser("~")).joinpath(".dbt/profiles.yml")
