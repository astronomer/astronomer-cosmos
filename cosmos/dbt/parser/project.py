"""
Used to parse and extract information from dbt projects.
"""
from __future__ import annotations

import os
import ast
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Set

import jinja2
import yaml

from cosmos.log import get_logger

logger = get_logger(__name__)


DBT_PY_MODEL_METHOD_NAME = "model"
DBT_PY_DEP_METHOD_NAME = "ref"
PYTHON_FILE_SUFFIX = ".py"


class DbtModelType(Enum):
    """
    Represents type of dbt unit (model, snapshot, seed, test)
    """

    DBT_MODEL = "model"
    DBT_SNAPSHOT = "snapshot"
    DBT_SEED = "seed"
    DBT_TEST = "test"


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
        prefixes: List[str] | None = None,
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
                if not any([element.startswith(config_type) for element in sql_configs]):
                    # if it does let's double-check against each prefix and add it
                    for prefix in prefixes:
                        # if the actual config doesn't exist in the sql_configs then add it
                        if not any([element.startswith(prefix) for element in sql_configs]):
                            sql_configs.add(config)
            else:
                sql_configs.add(config)

        return sql_configs


def extract_python_file_upstream_requirements(code: str) -> list[str]:
    """
    Given a dbt Python model source code, identify other dbt entities which are upstream requirements.

    This method tries to find a `model` function and `dbt.ref` calls within it. Return a list of upstream entities IDs.
    """
    source_code = ast.parse(code)

    upstream_entities = []
    model_function = None
    for node in source_code.body:
        if isinstance(node, ast.FunctionDef) and node.name == DBT_PY_MODEL_METHOD_NAME:
            model_function = node
            break

    if model_function:
        for item in ast.walk(model_function):
            if isinstance(item, ast.Call) and item.func.attr == DBT_PY_DEP_METHOD_NAME:  # type: ignore[attr-defined]
                upstream_entity_id = hasattr(item.args[-1], "value") and item.args[-1].value
                if upstream_entity_id:
                    upstream_entities.append(upstream_entity_id)

    return upstream_entities


@dataclass
class DbtModel:
    """
    Represents a single dbt model. (This class also hold snapshots)
    """

    # instance variables
    name: str
    type: DbtModelType
    path: Path
    operator_args: Dict[str, Any] = field(default_factory=dict)
    config: DbtModelConfig = field(default_factory=DbtModelConfig)

    def __post_init__(self) -> None:
        """
        Parses the file and extracts metadata (dependencies, tags, etc)
        """
        # first, get an empty config
        config = DbtModelConfig()
        var_args: Dict[str, Any] = self.operator_args.get("vars", {})

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

        elif self.type == DbtModelType.DBT_SEED or self.type == DbtModelType.DBT_TEST:
            return

        if self.path.suffix == PYTHON_FILE_SUFFIX:
            config.upstream_models = config.upstream_models.union(set(extract_python_file_upstream_requirements(code)))
        else:
            # get the dependencies
            env = jinja2.Environment()
            jinja2_ast = env.parse(code)
            # iterate over the jinja nodes to extract info
            for base_node in jinja2_ast.find_all(jinja2.nodes.Call):
                if hasattr(base_node.node, "name"):
                    try:
                        # check we have a ref - this indicates a dependency
                        if base_node.node.name == "ref":
                            # if it is, get the first argument
                            first_arg = base_node.args[0]
                            # if it contains vars, render the value of the var
                            if isinstance(first_arg, jinja2.nodes.Concat):
                                value = ""
                                for node in first_arg.nodes:
                                    if isinstance(node, jinja2.nodes.Const):
                                        value += node.value
                                    elif (
                                        isinstance(node, jinja2.nodes.Call)
                                        and isinstance(node.node, jinja2.nodes.Name)
                                        and isinstance(node.args[0], jinja2.nodes.Const)
                                        and node.node.name == "var"
                                    ):
                                        value += var_args[node.args[0].value]
                                config.upstream_models.add(value)
                            elif isinstance(first_arg, jinja2.nodes.Const):
                                # and add it to the config
                                config.upstream_models.add(first_arg.value)

                        # check if we have a config - this could contain tags
                        if base_node.node.name == "config":
                            # if it is, check if any kwargs are tags
                            for kwarg in base_node.kwargs:
                                for selector in self.config.config_types:
                                    extracted_config = self._extract_config(kwarg=kwarg, config_name=selector)
                                    config.config_selectors |= (
                                        set(extracted_config) if isinstance(extracted_config, (str, List)) else set()
                                    )
                    except KeyError as e:
                        logger.warning(f"Could not add upstream model for config in {self.path}: {e}")

        # set the config and set the parsed file flag to true
        self.config = config

    # TODO following needs coverage:
    def _extract_config(self, kwarg: Any, config_name: str) -> Any:
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
                logger.warning(f"Could not parse {config_name} from config in {self.path}: {e}")
                pass

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
    dbt_root_path: str | None = None
    dbt_models_dir: str | None = None
    dbt_snapshots_dir: str | None = None
    dbt_seeds_dir: str | None = None

    # private instance variables for managing state
    models: Dict[str, DbtModel] = field(default_factory=dict)
    snapshots: Dict[str, DbtModel] = field(default_factory=dict)
    seeds: Dict[str, DbtModel] = field(default_factory=dict)
    tests: Dict[str, DbtModel] = field(default_factory=dict)
    project_dir: Path = field(init=False)
    models_dir: Path = field(init=False)
    snapshots_dir: Path = field(init=False)
    seeds_dir: Path = field(init=False)

    operator_args: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        Initializes the parser.
        """
        if self.dbt_root_path is None:
            self.dbt_root_path = "/usr/local/airflow/dags/dbt"
        if self.dbt_models_dir is None:
            self.dbt_models_dir = "models"
        if self.dbt_snapshots_dir is None:
            self.dbt_snapshots_dir = "snapshots"
        if self.dbt_seeds_dir is None:
            self.dbt_seeds_dir = "seeds"

        # set the project and model dirs
        self.project_dir = Path(os.path.join(self.dbt_root_path, self.project_name))
        self.models_dir = self.project_dir / self.dbt_models_dir
        self.snapshots_dir = self.project_dir / self.dbt_snapshots_dir
        self.seeds_dir = self.project_dir / self.dbt_seeds_dir

        # crawl the models in the project
        for file_name in self.models_dir.rglob("*.sql"):
            self._handle_sql_file(file_name)

        # crawl the models in the project
        for file_name in self.models_dir.rglob("*.py"):
            self._handle_sql_file(file_name)

        # crawl the snapshots in the project
        for file_name in self.snapshots_dir.rglob("*.sql"):
            self._handle_sql_file(file_name)

        # crawl the seeds in the project
        for file_name in self.seeds_dir.rglob("*.csv"):
            self._handle_csv_file(file_name)

        # crawl the config files in the project
        for file_name in self.models_dir.rglob("*.yml"):
            self._handle_config_file(file_name)

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
            operator_args=self.operator_args,
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
                operator_args=self.operator_args,
            )
            # add the model to the project
            self.models[model.name] = model

        elif str(self.snapshots_dir) in str(path):
            model = DbtModel(
                name=model_name,
                type=DbtModelType.DBT_SNAPSHOT,
                path=path,
                operator_args=self.operator_args,
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
        if not config_dict:
            return

        for model in config_dict.get("models", []):
            model_name = model.get("name")

            # if the model doesn't exist, we can't do anything
            if not model_name:
                continue

            # tests
            for column in model.get("columns", []):
                for test in column.get("tests", []):
                    if not column.get("name"):
                        continue

                    # Get the test name
                    if not isinstance(test, str):
                        test = list(test.keys())[0]

                    test_model = DbtModel(
                        name=f"{test}_{column['name']}_{model_name}",
                        type=DbtModelType.DBT_TEST,
                        path=path,
                        operator_args=self.operator_args,
                        config=DbtModelConfig(upstream_models=set({model_name})),
                    )

                    self.tests[test_model.name] = test_model

            # config_selectors
            if model_name not in self.models:
                continue

            config_selectors = []
            for selector in DbtModelConfig.config_types:
                config_value = model.get("config", {}).get(selector)
                if config_value:
                    if isinstance(config_value, str):
                        config_selectors.append(f"{selector}:{config_value}")
                    else:
                        for item in config_value:
                            if item:
                                config_selectors.append(f"{selector}:{item}")

            # dbt default ensures "materialized:view" is set for all models if nothing is specified so that it will
            # work in a select/exclude list
            config_types = [
                selector_name for selector in config_selectors for selector_name in [selector.split(":")[0]]
            ]
            if "materialized" not in config_types:
                config_selectors.append("materialized:view")

            # then, get the model and merge the configs
            model = self.models[model_name]
            model.config = model.config + DbtModelConfig(config_selectors=set(config_selectors))
