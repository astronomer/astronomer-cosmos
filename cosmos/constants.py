import os
from enum import Enum
from pathlib import Path

import aenum
from packaging.version import Version

DBT_PROFILE_PATH = Path(os.path.expanduser("~")).joinpath(".dbt/profiles.yml")
DEFAULT_DBT_PROFILE_NAME = "cosmos_profile"
DEFAULT_DBT_TARGET_NAME = "cosmos_target"
DEFAULT_COSMOS_CACHE_DIR_NAME = "cosmos"
DBT_LOG_PATH_ENVVAR = "DBT_LOG_PATH"
DBT_LOG_DIR_NAME = "logs"
DBT_TARGET_PATH_ENVVAR = "DBT_TARGET_PATH"
DBT_TARGET_DIR_NAME = "target"
DBT_PARTIAL_PARSE_FILE_NAME = "partial_parse.msgpack"
DBT_MANIFEST_FILE_NAME = "manifest.json"
DBT_DEPENDENCIES_FILE_NAMES = {"packages.yml", "dependencies.yml"}
DBT_LOG_FILENAME = "dbt.log"
DBT_BINARY_NAME = "dbt"

DEFAULT_OPENLINEAGE_NAMESPACE = "cosmos"
OPENLINEAGE_PRODUCER = "https://github.com/astronomer/astronomer-cosmos/"

# Cosmos will not emit datasets for the following Airflow versions, due to a breaking change that's fixed in later Airflow 2.x versions
# https://github.com/apache/airflow/issues/39486
PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS = [Version("2.9.0"), Version("2.9.1")]


class LoadMode(Enum):
    """
    Supported ways to load a `dbt` project into a `DbtGraph` instance.
    """

    AUTOMATIC = "automatic"
    CUSTOM = "custom"
    DBT_LS = "dbt_ls"
    DBT_LS_FILE = "dbt_ls_file"
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
    AWS_EKS = "aws_eks"
    VIRTUALENV = "virtualenv"
    AZURE_CONTAINER_INSTANCE = "azure_container_instance"


class InvocationMode(Enum):
    """
    How the dbt command should be invoked.
    """

    SUBPROCESS = "subprocess"
    DBT_RUNNER = "dbt_runner"


class TestIndirectSelection(Enum):
    """
    Modes to configure the test behavior when performing indirect selection.
    """

    EAGER = "eager"
    CAUTIOUS = "cautious"
    BUILDABLE = "buildable"
    EMPTY = "empty"


class DbtResourceType(aenum.Enum):  # type: ignore
    """
    Type of dbt node.
    """

    MODEL = "model"
    SNAPSHOT = "snapshot"
    SEED = "seed"
    TEST = "test"
    SOURCE = "source"

    @classmethod
    def _missing_value_(cls, value):  # type: ignore
        aenum.extend_enum(cls, value.upper(), value.lower())
        return getattr(DbtResourceType, value.upper())


DEFAULT_DBT_RESOURCES = DbtResourceType.__members__.values()

# dbt test runs tests defined on models, sources, snapshots, and seeds.
# It expects that you have already created those resources through the appropriate commands.
# https://docs.getdbt.com/reference/commands/test
TESTABLE_DBT_RESOURCES = {DbtResourceType.MODEL, DbtResourceType.SOURCE, DbtResourceType.SNAPSHOT, DbtResourceType.SEED}
