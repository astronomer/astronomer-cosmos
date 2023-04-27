"""Used to parse and extract information from dbt projects."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Set

import jinja2
import yaml  # type: ignore
from jinja2.nodes import Call, Impossible, Keyword

logger = logging.getLogger(__name__)

SELECTORS = ["materialized", "schema", "tags"]


class DbtModelType(Enum):
    """
    Represents type of DBT unit (model, snapshot, seed)
    """

    DBT_MODEL = 1
    DBT_SNAPSHOT = 2
    DBT_SEED = 3


@dataclass
class DbtModelConfig:
    """
    Represents a single model config.
    """

    config_selectors: Set[str] = field(default_factory=set)
    upstream_models: Set[str] = field(default_factory=set)

    def __add__(self, other_config: DbtModelConfig) -> DbtModelConfig:
        """
        Add one config to another. Necessary because configs can come from different places.
        """
        # ensures proper order of operations between sql models and properties.yml
        result = self._config_resolution(
            existing_configs=self.config_selectors,
            new_configs=other_config.config_selectors,
        )
        # get the unique combination of each list
        return DbtModelConfig(
            config_selectors=result,
            upstream_models=self.upstream_models | other_config.upstream_models,
        )

    @staticmethod
    def _config_resolution(
        existing_configs: Set[str],
        new_configs: Set[str],
    ) -> Set[str]:
        """
        Assumes that configurations are passed in order of the most specific to the least specific.
        1. Config in the SQL Models.
        2. Config in schema.yml files.
        3. Config in project.yml files.
        """
        update_config_types = {"materialized", "schema"}
        existing_update_config_types = set()
        # Don't bother looking if we already have something from a more specific location.
        for existing_config in existing_configs:
            for update_config_type in update_config_types:
                if existing_config.startswith(update_config_type):
                    existing_update_config_types.add(update_config_type)
        update_config_types_to_search = update_config_types.difference(
            existing_update_config_types
        )
        new_update_configs = set()
        for update_config_type_to_search in update_config_types_to_search:
            for new_config in new_configs:
                if new_config.startswith(update_config_type_to_search):
                    new_update_configs.add(new_config)
        # Tags are always added, if they don't exist already.
        new_append_configs = {
            new_config for new_config in new_configs if new_config.startswith("tag")
        }
        new_update_configs.update(new_append_configs)
        existing_configs.update(new_update_configs)

        return existing_configs


@dataclass
class DbtModel:
    """
    Represents a single dbt model. (This class also hold snapshots)
    """

    # instance variables
    name: str
    type: DbtModelType
    path: Path
    config: DbtModelConfig = field(default_factory=DbtModelConfig)

    def __post_init__(self) -> None:
        """
        Parses the file and extracts metadata (dependencies, tags, etc)
        """
        # first, get an empty config
        config = DbtModelConfig()
        if self.type == DbtModelType.DBT_MODEL:
            # get the code from the file
            code = self.path.read_text()
        # we remove first and last line if the code is a snapshot
        elif self.type == DbtModelType.DBT_SNAPSHOT:
            code = self.path.read_text()
            snapshot_name = code.split("{%")[1]
            snapshot_name = snapshot_name.split("%}")[0]
            snapshot_name = snapshot_name.split(" ")[2]
            snapshot_name = snapshot_name.strip()
            self.name = snapshot_name
            code = code.split("%}")[1]
            code = code.split("{%")[0]
        elif self.type == DbtModelType.DBT_SEED:
            code = None
        # get the dependencies
        env = jinja2.Environment()
        ast = env.parse(code)
        # iterate over the jinja nodes to extract info
        for jinja_call_node in ast.find_all(Call):
            if hasattr(jinja_call_node.node, "name"):
                # check we have a ref - this indicates a dependency
                # TODO Do we care about source references? (unless we add source freshness) also a source takes 2 args.
                dependency_node_names = ("ref", "source")
                if jinja_call_node.node.name in dependency_node_names:
                    # if it is, get the first argument
                    first_arg = jinja_call_node.args[0]
                    if isinstance(first_arg, jinja2.nodes.Const):
                        # and add it to the config
                        config.upstream_models.add(first_arg.value)
                # check if we have a config - this could contain tags
                if jinja_call_node.node.name == "config":
                    # if it is, check if any kwargs are tags
                    for kwarg in jinja_call_node.kwargs:
                        for selector in SELECTORS:
                            extracted_config = self._extract_config(
                                kwarg=kwarg, config_name=selector
                            )
                            config.config_selectors |= (
                                set(extracted_config)
                                if isinstance(extracted_config, (str, List))
                                else set()
                            )

        self.config = config

    # TODO following needs coverage:
    def _extract_config(self, kwarg: Keyword, config_name: str):
        if hasattr(kwarg, "key") and kwarg.key == config_name:
            try:
                value = kwarg.value.as_const()
            except Impossible as e:
                logger.warning(
                    f"Could not parse {config_name} from config in {self.path}: {e}"
                )
                pass
            else:
                parsed_value = None
                if isinstance(value, List):
                    parsed_value = [f"{config_name}:{item}" for item in value]
                if isinstance(value, str):
                    parsed_value = [f"{config_name}:{value}"]
                return parsed_value

    def __repr__(self) -> str:
        """
        Returns the string representation of the model.
        """
        return f"DbtModel(name='{self.name}', type='{self.type}', path='{self.path}', config={self.config})"


@dataclass
class DbtProject:
    """
    Represents a single dbt project.
    """

    # required, user-specified instance variables
    project_name: str
    # optional, user-specified instance variables
    dbt_root_path: str = "/usr/local/airflow/dbt"
    dbt_models_dir: str = "models"
    dbt_snapshots_dir: str = "snapshots"
    dbt_seeds_dir: str = "seeds"
    # private instance variables for managing state
    models: Dict[str, DbtModel] = field(default_factory=dict)
    snapshots: Dict[str, DbtModel] = field(default_factory=dict)
    seeds: Dict[str, DbtModel] = field(default_factory=dict)
    project_dir: Path = field(init=False)
    models_dir: Path = field(init=False)
    snapshots_dir: Path = field(init=False)
    seeds_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        """
        Parses the dbt models, snapshots, seeds, config yml files and then finally dbt project yml file.
        For the configuration we go in order of the most specific to the least specific so:
            1. Config in models.
            2. Config in the schema.yml files, bottom up in the directory structure.
            3. Config in the project.yml file defined at the project level.
            4. If nothing is found for the materialization strategy then view is added for models only to mimic
                dbt behaviours.
        """
        self.project_dir = Path(os.path.join(self.dbt_root_path, self.project_name))
        self.models_dir = self.project_dir / self.dbt_models_dir
        self.snapshots_dir = self.project_dir / self.dbt_snapshots_dir
        self.seeds_dir = self.project_dir / self.dbt_seeds_dir
        for file_name in self.models_dir.rglob("*.sql"):
            self._handle_sql_file(file_name)
        for file_name in self.snapshots_dir.rglob("*.sql"):
            self._handle_sql_file(file_name)
        for file_name in self.seeds_dir.rglob("*.csv"):
            self._handle_csv_file(file_name)
        for root, dirs, files in os.walk(self.models_dir, topdown=False):
            for file in files:
                if file.endswith(".yml"):
                    path = Path(root) / file
                    self._handle_config_file(path)
        self._handle_project_config(self.project_dir / "dbt_project.yml")
        self._fill_gaps()

    def _handle_csv_file(self, path: Path) -> None:
        """
        Handles a single sql file.
        """
        # get the model name
        model_name = path.stem
        # construct the model object, which we'll use to store metadata
        model = DbtModel(
            name=model_name,
            type=DbtModelType.DBT_SEED,
            path=path,
        )
        # add the model to the project
        self.seeds[model_name] = model

    def _handle_sql_file(self, path: Path) -> None:
        """
        Handles a single sql file.
        """
        # get the model name
        model_name = path.stem
        # construct the model object, which we'll use to store metadata
        if str(self.models_dir) in str(path):
            model = DbtModel(
                name=model_name,
                type=DbtModelType.DBT_MODEL,
                path=path,
            )
            # add the model to the project
            self.models[model.name] = model
        elif str(self.snapshots_dir) in str(path):
            model = DbtModel(
                name=model_name,
                type=DbtModelType.DBT_SNAPSHOT,
                path=path,
            )
            # add the snapshot to the project
            self.snapshots[model.name] = model

    def _handle_config_file(self, path: Path) -> None:
        """
        Handles a single config file.
        """
        # parse the yml file
        config_dict = yaml.safe_load(path.read_text())
        # iterate over the models in the config
        if not config_dict.get("models"):
            return
        for model in config_dict["models"]:
            model_name = model.get("name")
            # if the model doesn't exist, we can't do anything
            if model_name not in self.models:
                continue
            # config_selectors
            config_selectors = []
            for selector in SELECTORS:
                config_value = model.get("config", {}).get(selector)
                if config_value:
                    if isinstance(config_value, str):
                        config_selectors.append(f"{selector}:{config_value}")
                    else:
                        [
                            config_selectors.append(f"{selector}:{item}")
                            for item in config_value
                            if item
                        ]
            # then, get the model and merge the configs
            model = self.models[model_name]
            model.config = model.config + DbtModelConfig(
                config_selectors=set(config_selectors)
            )

    def _handle_project_config(self, path: Path):
        """Gets any configuration from project.yml"""
        # parse the yml file
        config_dict = yaml.safe_load(path.read_text())
        # iterate over the models in the config
        models = config_dict.get("models", {})
        project_name = self.project_name
        project = models.get(project_name)
        if not project:
            return
        selectors = [f"+{selector}" for selector in SELECTORS]
        for model_folder, configs in project.items():
            self._handle_project_config_recursive(
                configs,
                selectors,
                Path(self.dbt_root_path) / project_name / "models" / model_folder,
            )

    def _handle_project_config_recursive(
        self, project_config: dict, selectors: list[str], project_path: Path
    ) -> None:
        """Goes bottom up to get tags against models (leaves to root).

        This is because the most specific wins for some types.
        As the tags are against directories and not models we need to understand which models are affected.

        :param project_config: This slice of the project.yml file.
        :param selectors: The configuration selectors that we're interested in.
        :param project_path: The path where the dbt models should be.
        :return: Nothing
        """
        # Get the selectors from children first
        for potential_path, potential_child_config in project_config.items():
            if not potential_path.startswith("+") and isinstance(
                potential_child_config, dict
            ):
                self._handle_project_config_recursive(
                    potential_child_config,
                    selectors,
                    project_path / potential_path,
                )
        # Get the selectors at this level
        config_selectors = set()
        for selector in selectors:
            config_value = project_config.get(selector)
            if config_value:
                if isinstance(config_value, str):
                    config_selectors.add(f"{selector[1:]}:{config_value}")
                if isinstance(config_value, list):
                    for item in config_value:
                        config_selectors.add(f"{selector[1:]}:{item}")
        if config_selectors:
            project_path_str = str(project_path)
            new_model_config = DbtModelConfig(config_selectors)
            for parsed_model_name, parsed_model in self.models.items():
                if str(parsed_model.path).startswith(project_path_str):
                    parsed_model.config = parsed_model.config + new_model_config

    def _fill_gaps(self):
        """
        dbt default ensures "materialized:view" is set for all models if nothing is specified so that it will
        work in a select/exclude list.
        This is not applicable to snapshots or seeds.
        """
        for model_name, model in self.models.items():
            if model.type == DbtModelType.DBT_MODEL:
                model.config = model.config + DbtModelConfig(
                    config_selectors={"materialized:view"}
                )
