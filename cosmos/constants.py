import os
from enum import Enum
from pathlib import Path


DBT_PROFILE_PATH = Path(os.path.expanduser("~")).joinpath(".dbt/profiles.yml")
DEFAULT_DBT_PROFILE_NAME = "cosmos_profile"
DEFAULT_DBT_TARGET_NAME = "cosmos_target"


class LoadMode(Enum):
    """
    Supported ways to load a `dbt` project into a `DbtGraph` instance.
    """

    AUTOMATIC = "automatic"
    CUSTOM = "custom"
    DBT_LS = "dbt_ls"
    DBT_MANIFEST = "dbt_manifest"


class TestBehavior(Enum):
    """
    Behavior of the tests.
    """

    NONE = "none"
    AFTER_EACH = "after_each"
    AFTER_ALL = "after_all"


class ExecutionMode(Enum):
    """
    Where the Cosmos tasks should be executed.
    """

    LOCAL = "local"
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    VIRTUALENV = "virtualenv"


# Rename to DbtResourceType
class DbtResourceType(Enum):
    """
    Type of dbt node.
    """

    MODEL = "model"
    SNAPSHOT = "snapshot"
    SEED = "seed"
    TEST = "test"
