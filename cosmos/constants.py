import os
from enum import Enum
from pathlib import Path
from typing import Callable, Dict

import aenum
from packaging.version import Version

BIGQUERY_PROFILE_TYPE = "bigquery"
DBT_PROFILE_PATH = Path(os.path.expanduser("~")).joinpath(".dbt/profiles.yml")
DBT_PROJECT_FILENAME = "dbt_project.yml"
DEFAULT_DBT_PROFILE_NAME = "cosmos_profile"
DEFAULT_DBT_TARGET_NAME = "cosmos_target"
DEFAULT_COSMOS_CACHE_DIR_NAME = "cosmos"
DEFAULT_TARGET_PATH = "target"
DBT_LOG_PATH_ENVVAR = "DBT_LOG_PATH"
DBT_LOG_DIR_NAME = "logs"
DBT_TARGET_PATH_ENVVAR = "DBT_TARGET_PATH"
DBT_TARGET_DIR_NAME = "target"
DBT_PARTIAL_PARSE_FILE_NAME = "partial_parse.msgpack"
DBT_MANIFEST_FILE_NAME = "manifest.json"
DBT_DEPENDENCIES_FILE_NAMES = {"packages.yml", "dependencies.yml"}
DBT_LOG_FILENAME = "dbt.log"
DBT_BINARY_NAME = "dbt"
DEFAULT_PROFILES_FILE_NAME = "profiles.yml"
PACKAGE_LOCKFILE_YML = "package-lock.yml"
DBT_DEFAULT_PACKAGES_FOLDER = "dbt_packages"

DEFAULT_OPENLINEAGE_NAMESPACE = "cosmos"
OPENLINEAGE_PRODUCER = "https://github.com/astronomer/astronomer-cosmos/"

# Cosmos will not emit datasets for the following Airflow versions, due to a breaking change that's fixed in later Airflow 2.x versions
# https://github.com/apache/airflow/issues/39486
PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS = [Version("2.9.0"), Version("2.9.1")]


AIRFLOW_OBJECT_STORAGE_PATH_URL_SCHEMES = ("s3", "gs", "gcs", "wasb", "abfs", "abfss", "az", "http", "https")


def _default_s3_conn() -> str:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return S3Hook.default_conn_name  # type: ignore[no-any-return]


def _default_gcs_conn() -> str:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    return GCSHook.default_conn_name  # type: ignore[no-any-return]


def _default_wasb_conn() -> str:
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    return WasbHook.default_conn_name  # type: ignore[no-any-return]


FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP: Dict[str, Callable[[], str]] = {
    "s3": _default_s3_conn,
    "gs": _default_gcs_conn,
    "adl": _default_wasb_conn,
    "abfs": _default_wasb_conn,
    "abfss": _default_wasb_conn,
}


class LoadMode(Enum):
    """
    Supported ways to load a `dbt` project into a `DbtGraph` instance.
    """

    AUTOMATIC = "automatic"
    CUSTOM = "custom"
    DBT_LS = "dbt_ls"
    DBT_LS_FILE = "dbt_ls_file"
    DBT_LS_CACHE = "dbt_ls_cache"
    DBT_MANIFEST = "dbt_manifest"


class TestBehavior(Enum):
    """
    Behavior of the tests.
    """

    __test__ = False

    BUILD = "build"
    NONE = "none"
    AFTER_EACH = "after_each"
    AFTER_ALL = "after_all"


class ExecutionMode(Enum):
    """
    Where the Cosmos tasks should be executed.
    """

    WATCHER = "watcher"
    LOCAL = "local"
    AIRFLOW_ASYNC = "airflow_async"
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    AWS_EKS = "aws_eks"
    AWS_ECS = "aws_ecs"
    VIRTUALENV = "virtualenv"
    AZURE_CONTAINER_INSTANCE = "azure_container_instance"
    GCP_CLOUD_RUN_JOB = "gcp_cloud_run_job"


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

    __test__ = False

    EAGER = "eager"
    CAUTIOUS = "cautious"
    BUILDABLE = "buildable"
    EMPTY = "empty"


class SourceRenderingBehavior(Enum):
    """
    Modes to configure the source rendering behavior.
    """

    NONE = "none"
    ALL = "all"
    WITH_TESTS_OR_FRESHNESS = "with_tests_or_freshness"


class DbtResourceType(aenum.Enum):  # type: ignore
    """
    Type of dbt node.
    """

    MODEL = "model"
    SNAPSHOT = "snapshot"
    SEED = "seed"
    TEST = "test"
    SOURCE = "source"
    EXPOSURE = "exposure"

    @classmethod
    def _missing_value_(cls, value):  # type: ignore
        aenum.extend_enum(cls, value.upper(), value.lower())
        return getattr(DbtResourceType, value.upper())


DEFAULT_DBT_RESOURCES = DbtResourceType.__members__.values()

# According to the dbt documentation (https://docs.getdbt.com/reference/commands/build), build also supports test nodes.
# However, in the context of Cosmos, we will run test nodes together with the respective models/seeds/snapshots nodes
SUPPORTED_BUILD_RESOURCES = [
    DbtResourceType.MODEL,
    DbtResourceType.SNAPSHOT,
    DbtResourceType.SEED,
]

# dbt test runs tests defined on models, sources, snapshots, and seeds.
# It expects that you have already created those resources through the appropriate commands.
# https://docs.getdbt.com/reference/commands/test
TESTABLE_DBT_RESOURCES = {DbtResourceType.MODEL, DbtResourceType.SOURCE, DbtResourceType.SNAPSHOT, DbtResourceType.SEED}

DBT_SETUP_ASYNC_TASK_ID = "dbt_setup_async"
DBT_TEARDOWN_ASYNC_TASK_ID = "dbt_teardown_async"

PRODUCER_WATCHER_TASK_ID = "dbt_producer_watcher"

TELEMETRY_URL = "https://astronomer.gateway.scarf.sh/astronomer-cosmos/{telemetry_version}/{cosmos_version}/{airflow_version}/{python_version}/{platform_system}/{platform_machine}/{event_type}/{status}/{dag_hash}/{task_count}/{cosmos_task_count}/{execution_modes}"
TELEMETRY_VERSION = "v2"
TELEMETRY_TIMEOUT = 1.0

_AIRFLOW3_MAJOR_VERSION = 3
