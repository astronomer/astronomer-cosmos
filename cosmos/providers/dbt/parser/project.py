"""
Used to parse and extract information from dbt projects.
"""
from __future__ import annotations

import os
import yaml  # type: ignore
import jinja2

from dataclasses import dataclass, field
from typing import Any, ClassVar
from pathlib import Path


@dataclass
class DbtModelConfig:
    """
    Represents a single model config.
    """

    tags: set[str] = field(default_factory=set)
    upstream_models: set[str] = field(default_factory=set)

    def __add__(self, other_config: DbtModelConfig) -> DbtModelConfig:
        """
        Add one config to another. Necessary because configs can come from different places
        """
        # get the unique combination of each list
        return DbtModelConfig(
            tags=self.tags | other_config.tags,
            upstream_models=self.upstream_models | other_config.upstream_models,
        )


@dataclass
class DbtModel:
    """
    Represents a single dbt model.
    """

    # instance variables
    name: str
    path: Path
    config: DbtModelConfig = field(default_factory=DbtModelConfig)

    # internal variables to manage state
    _parsed_file: bool = False

    def parse_file(self) -> None:
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

        # if we find a dependency, add it to the list. to do so, we use jinja to find
        # calls to the ref function, and then we get the first argument to the function
        for base_node in ast.find_all(jinja2.nodes.Call):
            # check if it's a ref
            if hasattr(base_node.node, "name") and base_node.node.name == "ref":
                # if it is, get the first argument
                first_arg = base_node.args[0]
                if isinstance(first_arg, jinja2.nodes.Const):
                    # and add it to the config
                    config.upstream_models.add(first_arg.value)

        # get the tags
        # TODO

        # set the config and set the parsed file flag to true
        self.config = config
        self._parsed_file = True

    @property
    def upstream_models(self) -> set[str]:
        # if we've already parsed the file, return the upstream models
        if self._parsed_file:
            return self.config.upstream_models

        # otherwise, we need to parse the file
        self.parse_file()

        # now we can return the upstream models
        return self.config.upstream_models

    @property
    def tags(self) -> set[str]:
        """
        Returns the tags for the model.
        """
        # if we've already parsed the file, return the tags
        if self._parsed_file:
            return self.config.tags

        # otherwise, we need to parse the file
        self.parse_file()

        # now we can return the tags
        return self.config.tags

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
    should_extract: bool = True

    # private instance variables for managing state
    _models: dict[str, DbtModel] = field(default_factory=dict)
    _project_dir: Path = field(init=False)
    _models_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        """
        Initializes the parser.
        """
        # set the project and model dirs
        self._project_dir = Path(os.path.join(self.dbt_root_path, self.project_name))
        self._models_dir = self._project_dir / "models"

        if self.should_extract:
            self.extract()

    def extract(self) -> None:
        """
        Extract metadata from the project, including the project config, sql files, and yml files.
        """
        # crawl the models in the project
        for file_name in self._models_dir.rglob("*.sql"):
            self._handle_sql_file(file_name)

        # crawl the config files in the project
        for file_name in self._models_dir.rglob("*.yml"):
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
        self._models[model_name] = model

    def _handle_config_file(self, path: Path) -> None:
        """
        Handles a single config file.
        """
        # parse the yml file
        config_dict = yaml.safe_load(path.read_text())

        # iterate over the models in the config
        if not config_dict.get("models"):
            return

        for config in config_dict["models"]:
            model_name = config.get("name")

            # if the model doesn't exist, we can't do anything
            if not model_name in self.models:
                continue

            # parse out the config fields we can recognize

            # 'tags' is either a string or list of strings
            tags = config.get("tags", [])
            if isinstance(tags, str):
                tags = [tags]

            # then, get the model and merge the configs
            model = self.models[model_name]
            model.config = model.config + DbtModelConfig(
                tags=set(tags),
            )

    # getters and setters
    @property
    def models(self) -> dict[str, DbtModel]:
        """
        Returns the models in the project.
        """
        return self._models

    @property
    def project_dir(self) -> Path:
        """
        Returns the project directory.
        """
        return self._project_dir
