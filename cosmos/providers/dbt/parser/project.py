"""
Used to parse and extract information from dbt projects.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

import jinja2
import yaml  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class DbtModel:
    """
    Represents a single dbt model.
    """

    # instance variables
    name: str
    path: Path
    config: Dict = None

    def __post_init__(self) -> None:
        """
        Parses the file and extracts metadata (dependencies, tags, etc)
        """

        # get the code from the file
        code = self.path.read_text()

        # get the dependencies
        env = jinja2.Environment()
        ast = env.parse(code)

        # iterate over the jinja nodes to extract info
        upstream_models, tags, materialized, schema = [], [], [], []
        for base_node in ast.find_all(jinja2.nodes.Call):
            if hasattr(base_node.node, "name"):
                # check we have a ref - this indicates a dependency
                if base_node.node.name == "ref":
                    # if it is, get the first argument
                    first_arg = base_node.args[0]
                    if isinstance(first_arg, jinja2.nodes.Const):
                        # and add it to the config
                        upstream_models.append(first_arg.value)

                # check if we have a config - this could contain tags

                if base_node.node.name == "config":
                    # if it is, check for the following configs # tags, # materialized
                    for kwarg in base_node.kwargs:
                        tags.append(
                            self._extract_config(kwarg, "tags")
                        ) if self._extract_config(kwarg, "tags") else None
                        materialized.append(
                            self._extract_config(kwarg, "materialized")
                        ) if self._extract_config(
                            kwarg, "materialized"
                        ) else None  # TODO: This shouldn't be a list, but a string
                        schema.append(
                            self._extract_config(kwarg, "schema")
                        ) if self._extract_config(
                            kwarg, "schema"
                        ) else None  # TODO: this shouldn't be a list, but a string

        # set the config and set the parsed file flag to true
        self.config = {
            "upstream_models": upstream_models,
            "tags": tags,
            "materialized": materialized,
            "schema": schema,
        }

    def _extract_config(self, kwarg, config_name):
        if hasattr(kwarg, "key") and kwarg.key == config_name:
            try:
                # try to convert it to a constant and get the value
                value = kwarg.value.as_const()
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
        self.models_dir = self.project_dir / "models"

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

            # parse out the config fields we can recognize

            # tags
            tags = model.get("config", {}).get("tags", [])
            if isinstance(tags, str):
                tags = [tags]

            # materialized
            materialized = model.get("config", {}).get("materialized", [])
            if isinstance(materialized, str):
                materialized = [materialized]

            # schema
            schema = model.get("config", {}).get("schema", [])
            if isinstance(schema, str):
                schema = [schema]

            # then, get the model and merge the configs
            model = self.models[model_name]
            model.config["tags"] = model.config["tags"] + tags
            model.config["materialized"] = model.config["materialized"] + materialized
            model.config["schema"] = model.config["schema"] + schema
