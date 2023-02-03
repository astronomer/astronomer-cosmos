"""
Used to parse and extract information from dbt projects.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Dict, List, Set

import jinja2
import yaml  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class DbtModelConfig:
    """
    Represents a single model config.
    """

    config_types: ClassVar[List[str]] = ["materialized", "schema", "tags"]
    config_selectors: Set[str] = field(default_factory=set)
    upstream_models: Set[str] = field(default_factory=set)

    def __add__(self, other_config: DbtModelConfig) -> DbtModelConfig:
        """
        Add one config to another. Necessary because configs can come from different places
        """
        # ensures proper order of operations between sql models and properties.yml
        result = self._config_selector_ooo(
            sql_configs=self.config_selectors,
            properties_configs=other_config.config_selectors,
        )

        # get the unique combination of each list
        return DbtModelConfig(
            config_selectors=result,
            upstream_models=self.upstream_models | other_config.upstream_models,
        )

    def _config_selector_ooo(
        self,
        sql_configs: Set[str],
        properties_configs: Set[str],
        prefixes: List[str] = None,
    ) -> Set[str]:
        """
        this will force values from the sql files to override whatever is in the properties.yml. So ooo:
        # 1. model sql files
        # 2. properties.yml files
        """

        # iterate on each properties.yml config
        # excluding tags because we just want to collect all of them
        if prefixes is None:
            prefixes = ["materialized", "schema"]

        for config in properties_configs:
            # identify the config_type and its associated value (i.e. materialized:table)
            config_type, value = config.split(":")
            # if the config_type matches is even in a list of prefixes then
            if config_type in prefixes:
                # make sure that it's prefix doesn't already exist in the sql configs
                if not any(
                    [element.startswith(config_type) for element in sql_configs]
                ):
                    # if it does let's double-check against each prefix and add it
                    for prefix in prefixes:
                        # if the actual config doesn't exist in the sql_configs then add it
                        if not any(
                            [element.startswith(prefix) for element in sql_configs]
                        ):
                            sql_configs.add(config)
            else:
                sql_configs.add(config)

        return sql_configs


@dataclass
class DbtModel:
    """
    Represents a single dbt model.
    """

    # instance variables
    name: str
    path: Path
    config: DbtModelConfig = field(default_factory=DbtModelConfig)

    def __post_init__(self) -> None:
        """
        Parses the file and extracts metadata (dependencies, tags, etc)
        """
        # first, get an empty config
        config = DbtModelConfig()

        # get the code from the file
        code = self.path.read_text()

        # get the dependencies
        env = jinja2.Environment()
        ast = env.parse(code)

        # iterate over the jinja nodes to extract info
        for base_node in ast.find_all(jinja2.nodes.Call):
            if hasattr(base_node.node, "name"):
                # check we have a ref - this indicates a dependency
                if base_node.node.name == "ref":
                    # if it is, get the first argument
                    first_arg = base_node.args[0]
                    if isinstance(first_arg, jinja2.nodes.Const):
                        # and add it to the config
                        config.upstream_models.add(first_arg.value)

                # check if we have a config - this could contain tags
                if base_node.node.name == "config":
                    # if it is, check if any kwargs are tags
                    for kwarg in base_node.kwargs:
                        for selector in self.config.config_types:
                            extracted_config = self._extract_config(
                                kwarg=kwarg, config_name=selector
                            )
                            config.config_selectors |= (
                                set(extracted_config)
                                if isinstance(extracted_config, (str, List))
                                else set()
                            )

        # set the config and set the parsed file flag to true
        self.config = config

    def _extract_config(self, kwarg, config_name: str):
        if hasattr(kwarg, "key") and kwarg.key == config_name:
            try:
                # try to convert it to a constant and get the value
                value = kwarg.value.as_const()
                if isinstance(value, List):
                    value = [f"{config_name}:{item}" for item in value]

                if isinstance(value, str):
                    value = [f"{config_name}:{value}"]

                return value

            except Exception as e:
                # if we can't convert it to a constant, we can't do anything with it
                logger.warning(
                    f"Could not parse {config_name} from config in {self.path}: {e}"
                )
                pass

    def __repr__(self) -> str:
        """
        Returns the string representation of the model.
        """
        return f"DbtModel(name='{self.name}', path='{self.path}', config={self.config})"


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

    # private instance variables for managing state
    models: Dict[str, DbtModel] = field(default_factory=dict)
    project_dir: Path = field(init=False)
    models_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        """
        Initializes the parser.
        """
        # set the project and model dirs
        self.project_dir = Path(os.path.join(self.dbt_root_path, self.project_name))
        self.models_dir = self.project_dir / self.dbt_models_dir

        # crawl the models in the project
        for file_name in self.models_dir.rglob("*.sql"):
            self._handle_sql_file(file_name)

        # crawl the config files in the project
        for file_name in self.models_dir.rglob("*.yml"):
            self._handle_config_file(file_name)

    def _handle_sql_file(self, path: Path) -> None:
        """
        Handles a single sql file.
        """
        # get the model name
        model_name = path.stem

        # construct the model object, which we'll use to store metadata
        model = DbtModel(
            name=model_name,
            path=path,
        )

        # add the model to the project
        self.models[model_name] = model

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
            for selector in self.models[model_name].config.config_types:
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

            # dbt default ensures "materialized:view" is set for all models if nothing is specified so that it will
            # work in a select/exclude list
            config_types = [
                selector_name
                for selector in config_selectors
                for selector_name in [selector.split(":")[0]]
            ]
            if "materialized" not in config_types:
                config_selectors.append("materialized:view")

            # then, get the model and merge the configs
            model = self.models[model_name]
            model.config = model.config + DbtModelConfig(
                config_selectors=set(config_selectors)
            )
