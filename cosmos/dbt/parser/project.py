"""
Used to parse and extract information from dbt projects.
"""

from __future__ import annotations

import ast
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar

import jinja2
import yaml

from cosmos.exceptions import CosmosValueError
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

    config_types: ClassVar[list[str]] = ["materialized", "schema", "tags"]
    config_selectors: set[str] = field(default_factory=set)
    upstream_models: set[str] = field(default_factory=set)

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
        sql_configs: set[str],
        properties_configs: set[str],
        prefixes: list[str] | None = None,
    ) -> set[str]:
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
    dbt_vars: dict[str, str] = field(default_factory=dict)
    config: DbtModelConfig = field(default_factory=DbtModelConfig)

    def __post_init__(self) -> None:
        """
        Parses the file and extracts metadata (dependencies, tags, etc)
        """
        if self.type == DbtModelType.DBT_SEED or self.type == DbtModelType.DBT_TEST:
            return

        config = DbtModelConfig()
        code = self.path.read_text()

        if self.type == DbtModelType.DBT_SNAPSHOT:
            snapshot_name = code.split("{%")[1]
            snapshot_name = snapshot_name.split("%}")[0]
            snapshot_name = snapshot_name.split(" ")[2]
            snapshot_name = snapshot_name.strip()
            self.name = snapshot_name
            code = code.split("%}")[1]
            code = code.split("{%")[0]

        if self.path.suffix == PYTHON_FILE_SUFFIX:
            config.upstream_models = config.upstream_models.union(set(extract_python_file_upstream_requirements(code)))
        else:
            upstream_models, extracted_config = self.extract_sql_file_requirements(code)
            config.upstream_models = config.upstream_models.union(set(upstream_models))
            config.config_selectors |= extracted_config

        self.config = config

    def extract_sql_file_requirements(self, code: str) -> tuple[list[str], set[str]]:
        """Extracts upstream models and config selectors from a dbt sql file."""
        # get the dependencies
        env = jinja2.Environment()
        jinja2_ast = env.parse(code)
        upstream_models = []
        config_selectors = set()
        # iterate over the jinja nodes to extract info
        for base_node in jinja2_ast.find_all(jinja2.nodes.Call):
            if hasattr(base_node.node, "name"):
                try:
                    # check we have a ref - this indicates a dependency
                    if base_node.node.name == "ref":
                        upstream_model = self._parse_jinja_ref_node(base_node)
                        if upstream_model:
                            upstream_models.append(upstream_model)
                    # check if we have a config - this could contain tags
                    if base_node.node.name == "config":
                        config_selectors |= self._parse_jinja_config_node(base_node)
                except KeyError as e:
                    logger.warning("Could not add upstream model for config in %s: %s", self.path, e)

        return upstream_models, config_selectors

    def _parse_jinja_ref_node(self, base_node: jinja2.nodes.Call) -> str | None:
        """Parses a jinja ref node."""
        # get the first argument
        first_arg = base_node.args[0]
        value = None
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
                    value += self.dbt_vars[node.args[0].value]
        elif isinstance(first_arg, jinja2.nodes.Const):
            # and add it to the config
            value = first_arg.value

        return value

    def _parse_jinja_config_node(self, base_node: jinja2.nodes.Call) -> set[str]:
        """Parses a jinja config node."""
        # check if any kwargs are tags
        selector_config = set()
        for kwarg in base_node.kwargs:
            for config_name in self.config.config_types:
                if hasattr(kwarg, "key") and kwarg.key == config_name:
                    extracted_config = self._extract_config(kwarg, config_name)
                    selector_config |= set(extracted_config) if isinstance(extracted_config, (str, list)) else set()
        return selector_config

    def _extract_config(self, kwarg: Any, config_name: str) -> Any:
        if hasattr(kwarg, "key") and kwarg.key == config_name:
            try:
                # try to convert it to a constant and get the value
                value = kwarg.value.as_const()
                if isinstance(value, list):
                    value = [f"{config_name}:{item}" for item in value]

                if isinstance(value, str):
                    value = [f"{config_name}:{value}"]

                return value

            except Exception as e:
                # if we can't convert it to a constant, we can't do anything with it
                logger.warning("Could not parse %s from config in %s: %s", config_name, self.path, e)
                pass

    def __repr__(self) -> str:
        """
        Returns the string representation of the model.
        """
        return f"DbtModel(name='{self.name}', type='{self.type}', path='{self.path}', config={self.config})"


@dataclass
class LegacyDbtProject:
    """
    Represents a single dbt project.
    """

    # required, user-specified instance variables
    project_name: str

    # optional, user-specified instance variables
    dbt_root_path: str | None = None
    dbt_models_dir: str | list[str] | None = None
    dbt_snapshots_dir: str | list[str] | None = None
    dbt_seeds_dir: str | list[str] | None = None

    # private instance variables for managing state
    models: dict[str, DbtModel] = field(default_factory=dict)
    snapshots: dict[str, DbtModel] = field(default_factory=dict)
    seeds: dict[str, DbtModel] = field(default_factory=dict)
    tests: dict[str, DbtModel] = field(default_factory=dict)
    project_dir: Path = field(init=False)
    models_dir: list[Path] = field(init=False)
    snapshots_dir: list[Path] = field(init=False)
    seeds_dir: list[Path] = field(init=False)

    dbt_vars: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        Initializes the parser.
        """
        self.dbt_root_path = self.dbt_root_path or "/usr/local/airflow/dags/dbt"
        # `is None` (not `or`) so an explicitly empty list (crawl nothing) isn't confused with "unset".
        dbt_models_dirs = ["models"] if self.dbt_models_dir is None else self._as_dir_list(self.dbt_models_dir)
        dbt_snapshots_dirs = (
            ["snapshots"] if self.dbt_snapshots_dir is None else self._as_dir_list(self.dbt_snapshots_dir)
        )
        dbt_seeds_dirs = ["seeds"] if self.dbt_seeds_dir is None else self._as_dir_list(self.dbt_seeds_dir)

        # set the project and model dirs
        self.project_dir = Path(os.path.join(self.dbt_root_path, self.project_name))
        self.models_dir = [self.project_dir / dbt_dir for dbt_dir in dbt_models_dirs]
        self.snapshots_dir = [self.project_dir / dbt_dir for dbt_dir in dbt_snapshots_dirs]
        self.seeds_dir = [self.project_dir / dbt_dir for dbt_dir in dbt_seeds_dirs]

        for models_dir in self.models_dir:
            self._crawl_models_dir(models_dir)

        # crawl the config files in the project. This runs as its own pass, after every models_dir
        # has been crawled above, so a schema.yml in one dir can apply config to a model that lives
        # in another dir — _handle_config_file only affects models that have already been parsed.
        for models_dir in self.models_dir:
            for file_name in models_dir.rglob("*.yml"):
                self._handle_config_file(file_name)

        # crawl the snapshots in the project
        for snapshots_dir in self.snapshots_dir:
            for file_name in snapshots_dir.rglob("*.sql"):
                self._handle_sql_file(file_name)

        # crawl the seeds in the project
        for seeds_dir in self.seeds_dir:
            for file_name in seeds_dir.rglob("*.csv"):
                self._handle_csv_file(file_name)

    def _crawl_models_dir(self, models_dir: Path) -> None:
        """Crawls a single models directory for .sql and .py model files."""
        for file_name in models_dir.rglob("*.sql"):
            if self._classify_dir(file_name) == DbtModelType.DBT_MODEL:
                self._handle_sql_file(file_name)

        # crawl the models in the project
        for file_name in models_dir.rglob("*.py"):
            if self._classify_dir(file_name) == DbtModelType.DBT_MODEL:
                self._handle_sql_file(file_name)

    @staticmethod
    def _as_dir_list(value: str | list[str] | None) -> list[str]:
        """Normalizes a single directory name or a list of directory names into a list."""
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return list(value)
        raise CosmosValueError(
            f"Expected a directory name (str) or a list of directory names, got {type(value).__name__}: {value!r}."
        )

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
            dbt_vars=self.dbt_vars,
        )
        # add the model to the project
        self.seeds[model_name] = model

    def _classify_dir(self, path: Path) -> DbtModelType | None:
        """Returns whether path lives under a configured models or snapshots dir; most specific dir wins."""
        candidates = [(models_dir, DbtModelType.DBT_MODEL) for models_dir in self.models_dir]
        candidates += [(snapshots_dir, DbtModelType.DBT_SNAPSHOT) for snapshots_dir in self.snapshots_dir]
        matches = [(dbt_dir, dbt_type) for dbt_dir, dbt_type in candidates if path.is_relative_to(dbt_dir)]
        if not matches:
            return None
        _, dbt_type = max(matches, key=lambda match: len(match[0].parts))
        return dbt_type

    def _handle_sql_file(self, path: Path) -> None:
        """
        Handles a single sql file.
        """
        # get the model name
        model_name = path.stem

        # construct the model object, which we'll use to store metadata
        resource_type = self._classify_dir(path)
        if resource_type == DbtModelType.DBT_MODEL:
            model = DbtModel(
                name=model_name,
                type=DbtModelType.DBT_MODEL,
                path=path,
                dbt_vars=self.dbt_vars,
            )
            # add the model to the project
            self.models[model.name] = model

        elif resource_type == DbtModelType.DBT_SNAPSHOT:
            model = DbtModel(
                name=model_name,
                type=DbtModelType.DBT_SNAPSHOT,
                path=path,
                dbt_vars=self.dbt_vars,
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

        for model_config in config_dict.get("models", []):
            model_name = model_config.get("name")

            # if the model doesn't exist, we can't do anything
            if not model_name:
                continue
            # tests
            model_tests = self._extract_model_tests(model_name, model_config, path)
            self.tests.update(model_tests)

            # config_selectors
            if model_name not in self.models:
                continue

            config_selectors = self._extract_config_selectors(model_config)

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

    def _extract_model_tests(
        self, model_name: str, model_config: dict[str, list[dict[str, dict[str, list[str]]]]], path: Path
    ) -> dict[str, DbtModel]:
        """Extracts tests from a dbt config file model."""
        tests = {}
        for column in model_config.get("columns", []):
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
                    dbt_vars=self.dbt_vars,
                    config=DbtModelConfig(upstream_models={model_name}),
                )
                tests[test_model.name] = test_model
        return tests

    def _extract_config_selectors(self, model_config: dict[str, dict[str, str | list[str]]]) -> list[str]:
        """Extracts config selectors from a dbt config file model."""
        config_selectors = []
        for selector in DbtModelConfig.config_types:
            config_value = model_config.get("config", {}).get(selector)
            if config_value:
                if isinstance(config_value, str):
                    config_selectors.append(f"{selector}:{config_value}")
                else:
                    for item in config_value:
                        if item:
                            config_selectors.append(f"{selector}:{item}")
        return config_selectors
