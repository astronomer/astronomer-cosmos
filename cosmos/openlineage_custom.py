import os
from typing import Any, Dict, Optional, Tuple
from pathlib import Path

import airflow
from airflow.configuration import conf
from jinja2 import Environment
from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor, SkipUndefined
from openlineage.common.provider.dbt.processor import Adapter, DbtEvents

from cosmos.constants import DEFAULT_OPENLINEAGE_NAMESPACE, OPENLINEAGE_PRODUCER
from cosmos.log import get_logger


logger = get_logger(__name__)


try:
    namespace = conf.get("openlineage", "namespace")
except airflow.exceptions.AirflowConfigException:
    namespace = os.getenv("OPENLINEAGE_NAMESPACE", DEFAULT_OPENLINEAGE_NAMESPACE)


class CustomDbtLocalArtifactProcessor(DbtLocalArtifactProcessor):
    """
    This class allows us to build Airflow Dataset URIs during DAG parsing, without the need to have the file
    `run_results.json`. This is necessary until Airflow supports setting inlets and outlets during task execution:
    https://github.com/apache/airflow/issues/34206
    """
    should_raise_on_unsupported_command = False

    def __init__(
            self,
            profile_filepath: Path,
            manifest_dir: Path,
            profile_name: str,
            target: str,
            command: str = "bla",
            env_vars: dict[str, str] | None = None,
            *args,
            **kwargs,
    ):
        self.manifest_path = manifest_dir / "manifest.json"
        self.profile_filepath = profile_filepath
        self.profile_name = profile_name
        self.target = target
        self.command = command
        self.env_vars = env_vars or {}
        os.environ = {**os.environ, **env_vars}
        super().__init__(
            producer=OPENLINEAGE_PRODUCER,
            job_namespace=namespace,
            *args,
            **kwargs
        )

    def extract_adapter_type(self, profile: Dict):
        profile_type = profile["type"]
        try:
            self.adapter_type = Adapter[profile_type.upper()]
        except KeyError:
            self.adapter_type = f"{profile_type.lower()}://{profile['host']}:{profile['port']}"
    def get_dbt_metadata(self) -> Tuple[
        Dict[Any, Any], Optional[Dict[Any, Any]], Dict[Any, Any], Optional[Dict[Any, Any]]
    ]:
        """
        This is a simplified version compared to the original implementation, since it ignores catalog.json and
        run_result.json files, which are not usually available in a dbt project folder during Cosmos DAG parsing.
        """
        catalog = None
        run_result = None
        manifest = self.load_metadata(
            self.manifest_path, [2, 3, 4, 5, 6, 7], logger
        )
        profile = self.load_yaml_with_jinja(
            self.profile_filepath,
            include_section=["default"],
        )["default"]
        profile = profile["outputs"]["dev"]
        return manifest, run_result, profile, catalog

    def parse(self) -> DbtEvents:
        """
        Parse dbt manifest to build Airflow inlets and outlets Dataset URIs.
        This is a simplified version of the parent class implementation.
        """
        manifest, run_result, profile, catalog = self.get_dbt_metadata()

        #self.command = run_result["args"]["which"]

        self.extract_adapter_type(profile)

        nodes = {}
        # Filter nodes
        for name, node in manifest["nodes"].items():
            if any(
                    name.startswith(prefix) for prefix in ("model.", "test.", "snapshot.")
            ):
                nodes[name] = node

        events = DbtEvents()
        breakpoint
        #return events
        #name.startswith(prefix)
        for prefix in ("model.", "source.", "snapshot.")
        run["unique_id"]

        #dataset_uri = output.namespace + "/" + output.name
        #events.completes