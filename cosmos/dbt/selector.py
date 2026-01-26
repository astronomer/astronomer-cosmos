from __future__ import annotations

import copy
import hashlib
import inspect
import re
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from cosmos.constants import DbtResourceType
from cosmos.exceptions import CosmosValueError
from cosmos.log import get_logger

if TYPE_CHECKING:
    from cosmos.dbt.graph import DbtNode


CONFIG_META_PATH = "meta"
SUPPORTED_CONFIG = ["materialized", "schema", "tags", CONFIG_META_PATH]
PATH_SELECTOR = "path:"
TAG_SELECTOR = "tag:"
CONFIG_SELECTOR = "config."
SOURCE_SELECTOR = "source:"
EXPOSURE_SELECTOR = "exposure:"
RESOURCE_TYPE_SELECTOR = "resource_type:"
EXCLUDE_RESOURCE_TYPE_SELECTOR = "exclude_resource_type:"
PLUS_SELECTOR = "+"
AT_SELECTOR = "@"
GRAPH_SELECTOR_REGEX = r"^(@|[0-9]*\+)?([^\+]+)(\+[0-9]*)?$|"

logger = get_logger(__name__)


def _check_nested_value_in_dict(dict_: dict[Any, Any], pattern: str) -> bool:
    """
    Given a dictionary dict_, identify if the pattern defined in pattern happens on the dictionary.

    :param dict_: a dictionary
    :param pattern: a string containing a dotted path that reference dictionary keys and a value (e.g.  "config.meta.frequency:daily")
    :return: True/False if the desired pattern happens or not in the dictionary


    Example:
        dict_ = {
            "config": {
                "meta": {
                    "frequency": "daily"
                }
            }
        pattern = "config.meta.frequency:daily"
        assert self._check_nested_value_in_dict(dict_, pattern)

    """
    keys, expected_value = pattern.split(":")
    keys_values = keys.split(".")

    current: dict[Any, Any] | str = dict_
    for key in keys_values:
        if isinstance(current, dict):
            if key in current:
                current = current[key]
            else:
                return False  # Key path doesn't exist

    return current == expected_value


@dataclass
class GraphSelector:
    """
    Implements dbt graph operator selectors:
        model_a
        +model_b
        model_c+
        +model_d+
        2+model_e
        model_f+3
        @model_g
        +/path/to/model_g+
        path:/path/to/model_h+
        +tag:nightly
        +config.materialized:view
        resource_type:resource_name
        source:source_name
        exclude_resource_type:resource_name
        exposure:exposure_name

    https://docs.getdbt.com/reference/node-selection/graph-operators
    """

    node_name: str
    precursors: str | None
    descendants: str | None
    at_operator: bool = False

    @property
    def precursors_depth(self) -> int:
        """
        Calculates the depth/degrees/generations of precursors (parents).
        Return:
            -1: if it should return all the generations of precursors
            0: if it shouldn't return any precursors
            >0: upperbound number of parent generations
        """
        if self.at_operator:
            return -1
        if not self.precursors:
            return 0
        if self.precursors == "+":
            return -1
        else:
            return int(self.precursors[:-1])

    @property
    def descendants_depth(self) -> int:
        """
        Calculates the depth/degrees/generations of descendants (children).
        Return:
            -1: if it should return all the generations of children
            0: if it shouldn't return any children
            >0: upperbound of children generations
        """
        if not self.descendants:
            return 0
        if self.descendants == "+":
            return -1
        else:
            return int(self.descendants[1:])

    @staticmethod
    def parse(text: str) -> GraphSelector | None:
        """
        Parse a string and identify if there are graph selectors, including the desired node name, descendants and
        precursors. Return a GraphSelector instance if the pattern matches.
        """
        regex_match = re.search(GRAPH_SELECTOR_REGEX, text)
        if regex_match:
            precursors, node_name, descendants = regex_match.groups()
            if "/" in node_name and not node_name.startswith(PATH_SELECTOR):
                node_name = f"{PATH_SELECTOR}{node_name}"

            at_operator = precursors == AT_SELECTOR
            if at_operator:
                precursors = None
                descendants = "+"  # @ implies all descendants

            return GraphSelector(node_name, precursors, descendants, at_operator)
        return None

    def select_node_precursors(self, nodes: dict[str, DbtNode], root_id: str, selected_nodes: set[str]) -> None:
        """
        Parse original nodes and add the precursor nodes related to this config to the selected_nodes set.

        :param nodes: Original dbt nodes list
        :param root_id: Unique identifier of self.node_name
        :param selected_nodes: Set where precursor nodes will be added to.
        """
        if self.precursors or self.at_operator:
            depth = self.precursors_depth
            previous_generation = {root_id}
            processed_nodes = set()
            while depth and previous_generation:
                new_generation: set[str] = set()
                for node_id in previous_generation:
                    if node_id not in processed_nodes:
                        new_generation.update(set(nodes[node_id].depends_on))
                        processed_nodes.add(node_id)
                selected_nodes.update(new_generation)
                previous_generation = new_generation
                depth -= 1

    def select_node_descendants(self, nodes: dict[str, DbtNode], root_id: str, selected_nodes: set[str]) -> None:
        """
        Parse original nodes and add the descendant nodes related to this config to the selected_nodes set.

        :param nodes: Original dbt nodes list
        :param root_id: Unique identifier of self.node_name
        :param selected_nodes: Set where descendant nodes will be added to.
        """
        if self.descendants:
            children_by_node = defaultdict(set)
            # Index nodes by parent id
            # We could optimize by doing this only once for the dbt project and giving it
            # as a parameter to the GraphSelector
            for node_id, node in nodes.items():
                for parent_id in node.depends_on:
                    children_by_node[parent_id].add(node_id)

            depth = self.descendants_depth
            previous_generation = {root_id}
            processed_nodes = set()
            while depth and previous_generation:
                new_generation: set[str] = set()
                for node_id in previous_generation:
                    if node_id not in processed_nodes:
                        new_generation.update(children_by_node[node_id])
                        processed_nodes.add(node_id)
                selected_nodes.update(new_generation)
                previous_generation = new_generation
                depth -= 1

    def filter_nodes(self, nodes: dict[str, DbtNode]) -> set[str]:  # noqa: C901
        """
        Given a dictionary with the original dbt project nodes, applies the current graph selector to
        identify the subset of nodes that matches the selection criteria.

        :param nodes: dbt project nodes
        :return: set of node ids that matches current graph selector
        """
        selected_nodes: set[str] = set()
        root_nodes: set[str] = set()
        # Index nodes by name, we can improve performance by doing this once
        # for multiple GraphSelectors
        if PATH_SELECTOR in self.node_name:
            path_selection = self.node_name[len(PATH_SELECTOR) :].rstrip("*")
            root_nodes.update({node_id for node_id, node in nodes.items() if path_selection in str(node.file_path)})

        elif TAG_SELECTOR in self.node_name:
            tag_selection = self.node_name[len(TAG_SELECTOR) :]
            root_nodes.update({node_id for node_id, node in nodes.items() if tag_selection in node.tags})

        elif SOURCE_SELECTOR in self.node_name:
            source_selection = self.node_name[len(SOURCE_SELECTOR) :]

            # match node.resource_type == SOURCE, node.resource_name == source_selection
            root_nodes.update(
                {
                    node_id
                    for node_id, node in nodes.items()
                    if node.resource_type == DbtResourceType.SOURCE and node.resource_name == source_selection
                }
            )

        elif EXPOSURE_SELECTOR in self.node_name:
            exposure_selection = self.node_name[len(EXPOSURE_SELECTOR) :]

            # match node.resource_type == EXPOSURE, node.resource_name == exposure_selection
            root_nodes.update(
                {
                    node_id
                    for node_id, node in nodes.items()
                    if node.resource_type == DbtResourceType.EXPOSURE and node.resource_name == exposure_selection
                }
            )

        elif CONFIG_SELECTOR in self.node_name:
            config_selection_key, config_selection_value = self.node_name[len(CONFIG_SELECTOR) :].split(":")
            # currently tags, materialized, schema and meta are the only supported config keys
            # logic is separated into two conditions because the config 'tags' contains a
            # list of tags, the config 'materialized' & 'schema' contain strings and meta contains dictionaries
            if config_selection_key == "tags":
                root_nodes.update(
                    {
                        node_id
                        for node_id, node in nodes.items()
                        if config_selection_value in node.config.get(config_selection_key, [])
                    }
                )
            elif config_selection_key in (
                "materialized",
                "schema",
            ):
                root_nodes.update(
                    {
                        node_id
                        for node_id, node in nodes.items()
                        if config_selection_value == node.config.get(config_selection_key, "")
                    }
                )
            elif config_selection_key.startswith(CONFIG_META_PATH):
                root_nodes.update(
                    {
                        node_id
                        for node_id, node in nodes.items()
                        if _check_nested_value_in_dict(node.config, f"{config_selection_key}:{config_selection_value}")
                    }
                )
            else:
                logger.warning("Unsupported config key selector: %s", config_selection_key)
        else:
            node_by_name = {}
            for node_id, node in nodes.items():
                node_by_name[node.name] = node_id

            node_name_patched = self.node_name.replace(".", "_")

            if node_name_patched in node_by_name:
                root_id = node_by_name[node_name_patched]
                root_nodes.add(root_id)
            else:
                logger.warning(f"Selector {self.node_name} not found.")
                return selected_nodes

        selected_nodes.update(root_nodes)

        self._select_nodes(nodes, root_nodes, selected_nodes)

        return selected_nodes

    def _select_nodes(self, nodes: dict[str, DbtNode], root_nodes: set[str], selected_nodes: set[str]) -> None:
        """
        Handle selection of nodes based on the graph selector configuration.

        :param nodes: dbt project nodes
        :param root_nodes: Set of root node ids
        :param selected_nodes: Set where selected nodes will be added to.
        """
        if self.at_operator:
            descendants: set[str] = set()
            # First get all descendants
            for root_id in root_nodes:
                self.select_node_descendants(nodes, root_id, descendants)
            selected_nodes.update(descendants)

            # Get ancestors for root nodes and all descendants
            for node_id in root_nodes | descendants:
                self.select_node_precursors(nodes, node_id, selected_nodes)
        else:
            # Normal selection
            for root_id in root_nodes:
                self.select_node_precursors(nodes, root_id, selected_nodes)
                self.select_node_descendants(nodes, root_id, selected_nodes)


class SelectorConfig:
    """
    Represents a select/exclude statement.
    Supports to load it from a string.
    """

    def __init__(self, project_dir: Path | None, statement: str):
        """
        Create a selector config file.

        :param project_dir: Directory to a dbt project
        :param statement: dbt statement as passed within select and exclude arguments

        References:
        https://docs.getdbt.com/reference/node-selection/syntax
        https://docs.getdbt.com/reference/node-selection/yaml-selectors
        """
        self.project_dir = project_dir
        self.paths: list[Path] = []
        self.tags: list[str] = []
        self.config: dict[str, str] = {}
        self.other: list[str] = []
        self.graph_selectors: list[GraphSelector] = []
        self.sources: list[str] = []
        self.exposures: list[str] = []
        self.resource_types: list[str] = []
        self.exclude_resource_types: list[str] = []
        self.load_from_statement(statement)

    @property
    def is_empty(self) -> bool:
        return not (
            self.paths
            or self.tags
            or self.config
            or self.graph_selectors
            or self.other
            or self.sources
            or self.exposures
            or self.resource_types
            or self.exclude_resource_types
        )

    def load_from_statement(self, statement: str) -> None:
        """
        Load in-place select parameters.
        Raises an exception if they are not yet implemented in Cosmos.

        :param statement: dbt statement as passed within select and exclude arguments

        References:
        https://docs.getdbt.com/reference/node-selection/syntax
        https://docs.getdbt.com/reference/node-selection/yaml-selectors
        """
        items = statement.split(",")

        for item in items:
            regex_match = re.search(GRAPH_SELECTOR_REGEX, item)
            if regex_match:
                precursors, node_name, descendants = regex_match.groups()
                if node_name is None:
                    ...
                elif precursors or descendants:
                    self._parse_unknown_selector(item)
                else:
                    self._handle_no_precursors_or_descendants(item, node_name)

    def _handle_no_precursors_or_descendants(self, item: str, node_name: str) -> None:
        if node_name.startswith(PATH_SELECTOR):
            self._parse_path_selector(item)
        elif "/" in node_name:
            self._parse_path_selector(f"{PATH_SELECTOR}{node_name}")
        elif node_name.startswith(TAG_SELECTOR):
            self._parse_tag_selector(item)
        elif node_name.startswith(CONFIG_SELECTOR):
            self._parse_config_selector(item)
        elif node_name.startswith(SOURCE_SELECTOR):
            self._parse_source_selector(item)
        elif node_name.startswith(EXPOSURE_SELECTOR):
            self._parse_exposure_selector(item)
        elif node_name.startswith(RESOURCE_TYPE_SELECTOR):
            self._parse_resource_type_selector(item)
        elif node_name.startswith(EXCLUDE_RESOURCE_TYPE_SELECTOR):
            self._parse_exclude_resource_type_selector(item)
        else:
            self._parse_unknown_selector(item)

    def _parse_unknown_selector(self, item: str) -> None:
        if item:
            graph_selector = GraphSelector.parse(item)
            if graph_selector is not None:
                self.graph_selectors.append(graph_selector)
            else:
                self.other.append(item)
                logger.warning("Unsupported select statement: %s", item)

    def _parse_config_selector(self, item: str) -> None:
        index = len(CONFIG_SELECTOR)
        key, value = item[index:].split(":")

        if key in SUPPORTED_CONFIG or key.startswith(CONFIG_META_PATH):
            self.config[key] = value

    def _parse_tag_selector(self, item: str) -> None:
        index = len(TAG_SELECTOR)
        self.tags.append(item[index:])

    def _parse_path_selector(self, item: str) -> None:
        index = len(PATH_SELECTOR)
        if self.project_dir:
            self.paths.append(self.project_dir / Path(item[index:].rstrip("*")))
        else:
            self.paths.append(Path(item[index:]))

    def _parse_resource_type_selector(self, item: str) -> None:
        index = len(RESOURCE_TYPE_SELECTOR)
        resource_type_value = item[index:].strip()
        self.resource_types.append(resource_type_value)

    def _parse_exclude_resource_type_selector(self, item: str) -> None:
        index = len(EXCLUDE_RESOURCE_TYPE_SELECTOR)
        resource_type_value = item[index:].strip()
        self.exclude_resource_types.append(resource_type_value)

    def _parse_source_selector(self, item: str) -> None:
        index = len(SOURCE_SELECTOR)
        source_name = item[index:].strip()
        self.sources.append(source_name)

    def _parse_exposure_selector(self, item: str) -> None:
        index = len(EXPOSURE_SELECTOR)
        exposure_name = item[index:].strip()
        self.exposures.append(exposure_name)

    def __repr__(self) -> str:
        return (
            "SelectorConfig("
            + f"paths={self.paths}, "
            + f"tags={self.tags}, "
            + f"config={self.config}, "
            + f"sources={self.sources}, "
            + f"resource={self.resource_types}, "
            + f"exposures={self.exposures}, "
            + f"exclude_resource={self.exclude_resource_types}, "
            + f"other={self.other}, "
            + f"graph_selectors={self.graph_selectors})"
        )


class NodeSelector:
    """
    Class to select nodes based on a selector config.

    :param nodes: Dictionary mapping dbt nodes (node.unique_id to node)
    :param config: User-defined select statements
    """

    def __init__(self, nodes: dict[str, DbtNode], config: SelectorConfig) -> None:
        self.nodes = nodes
        self.config = config
        self.selected_nodes: set[str] = set()

    @property
    def select_nodes_ids_by_intersection(self) -> set[str]:
        """
        Return a list of node ids which matches the configuration defined in config.

        References:
        https://docs.getdbt.com/reference/node-selection/syntax
        https://docs.getdbt.com/reference/node-selection/yaml-selectors
        """
        if self.config.is_empty:
            return set(self.nodes.keys())

        selected_nodes: set[str] = set()
        self.visited_nodes: set[str] = set()

        if self.config.graph_selectors:
            graph_selected_nodes = self.select_by_graph_operator()
            for node_id in graph_selected_nodes:
                node = self.nodes[node_id]
                # Since the method below changes the tags of test nodes, it can lead to incorrect
                # results during the application of graph selectors. Therefore, it is being run within
                # nodes previously selected
                # This solves https://github.com/astronomer/astronomer-cosmos/pull/1466
                if self._should_include_node(node_id, node):
                    selected_nodes.add(node_id)
        else:
            for node_id, node in self.nodes.items():
                if self._should_include_node(node_id, node):
                    selected_nodes.add(node_id)

        self.selected_nodes = selected_nodes
        return selected_nodes

    def _should_include_based_on_meta(self, node: DbtNode, config: dict[Any, Any]) -> bool:

        # Deal with meta properties
        selector_meta_patterns = {key: value for key, value in config.items() if key.startswith(CONFIG_META_PATH)}
        if selector_meta_patterns:
            for key, value in selector_meta_patterns.items():
                if not _check_nested_value_in_dict(node.config, f"{key}:{value}"):
                    return False
                config.pop(key)
        return True

    def _should_include_based_on_non_meta_and_non_tag_config(self, node: DbtNode, config: dict[Any, Any]) -> bool:
        node_non_meta_or_tag_config = {
            key: value
            for key, value in node.config.items()
            if key in SUPPORTED_CONFIG and key != "tag" and not key.startswith(CONFIG_META_PATH)
        }

        if not (config.items() <= node_non_meta_or_tag_config.items()):
            return False

        if not self._is_config_subset(node_non_meta_or_tag_config):
            return False
        return True

    def _should_include_node(self, node_id: str, node: DbtNode) -> bool:
        """
        Checks if a single node should be included. Only runs once per node with caching."""
        logger.debug("Inspecting if the node <%s> should be included.", node_id)
        if node_id in self.visited_nodes:
            return node_id in self.selected_nodes

        self.visited_nodes.add(node_id)

        # Disclaimer: this method currently copies the tags from parent nodes to children nodes
        # that are tests. This can lead to incorrect results in graph node selectors such as reported in
        # https://github.com/astronomer/astronomer-cosmos/pull/1466
        if node.resource_type == DbtResourceType.TEST and node.depends_on and len(node.depends_on) > 0:
            node.tags = getattr(self.nodes.get(node.depends_on[0]), "tags", [])
            logger.debug(
                "The test node <%s> inherited these tags from the parent node <%s>: %s",
                node_id,
                node.depends_on[0],
                node.tags,
            )

        if not self._is_tags_subset(node):
            logger.debug("Excluding node <%s>", node_id)
            return False

        # Remove 'tags' as they've already been filtered for
        config_copy = copy.deepcopy(self.config.config)
        config_copy.pop("tags", None)

        # Handle other config attributes, including meta and general config
        if not self._should_include_based_on_meta(
            node, config_copy
        ) or not self._should_include_based_on_non_meta_and_non_tag_config(node, config_copy):
            return False

        if self.config.paths and not self._is_path_matching(node):
            return False

        if self.config.resource_types and not self._is_resource_type_matching(node):
            return False

        if self.config.exclude_resource_types and self._is_exclude_resource_type_matching(node):
            return False

        if self.config.sources and not self._is_source_matching(node):
            return False

        if self.config.exposures and not self._is_exposure_matching(node):
            return False

        return True

    def _is_resource_type_matching(self, node: DbtNode) -> bool:
        """Checks if the node's resource type is a subset of the config's resource type."""
        if node.resource_type.value not in self.config.resource_types:
            return False
        return True

    def _is_exclude_resource_type_matching(self, node: DbtNode) -> bool:
        """Checks if the node's resource type is a subset of the config's exclude resource type."""
        return node.resource_type.value in self.config.exclude_resource_types

    def _is_source_matching(self, node: DbtNode) -> bool:
        """Checks if the node's source is a subset of the config's source."""
        if node.resource_type != DbtResourceType.SOURCE:
            return False
        if node.resource_name not in self.config.sources:
            return False
        return True

    def _is_exposure_matching(self, node: DbtNode) -> bool:
        """Checks if the node's exposure is a subset of the config's exposure."""
        if node.resource_type != DbtResourceType.EXPOSURE:
            return False
        if node.resource_name not in self.config.exposures:
            return False
        return True

    def _is_tags_subset(self, node: DbtNode) -> bool:
        """Checks if the node's tags are a subset of the config's tags."""
        if not (set(self.config.tags) <= set(node.tags)):
            return False
        return True

    def _is_config_subset(self, node_config: dict[str, Any]) -> bool:
        """Checks if the node's config is a subset of the config's config."""
        config_tags = self.config.config.get("tags")
        if config_tags and config_tags not in node_config.get("tags", []):
            return False
        return True

    def _is_path_matching(self, node: DbtNode) -> bool:
        """Checks if the node's path is a subset of the config's paths."""
        for filter_path in self.config.paths:
            if filter_path in node.file_path.parents or filter_path == node.file_path:
                return True

        # if it's a test coming from a schema.yml file, check the model's file_path
        if node.resource_type == DbtResourceType.TEST and node.file_path.name == "schema.yml":
            # try to get the corresponding model from node.depends_on
            if len(node.depends_on) == 1:
                model_node = self.nodes.get(node.depends_on[0])
                if model_node:
                    return self._should_include_node(node.depends_on[0], model_node)
        return False

    def select_by_graph_operator(self) -> set[str]:
        """
        Return a list of node ids which match the configuration defined in the config.

        Return all nodes that are parents (or parents from parents) of the root defined in the configuration.

        References:
        https://docs.getdbt.com/reference/node-selection/syntax
        https://docs.getdbt.com/reference/node-selection/yaml-selectors
        """
        selected_nodes_by_selector: list[set[str]] = []

        for graph_selector in self.config.graph_selectors:
            selected_nodes_by_selector.append(graph_selector.filter_nodes(self.nodes))
        return set.intersection(*selected_nodes_by_selector)


def retrieve_by_label(statement_list: list[str], label: str) -> set[str]:
    """
    Return a set of values associated with a label.

    Example:
        >>> values = retrieve_by_label(["path:/tmp,tag:a", "tag:b,path:/home"])
        >>> values
        {"a", "b"}
    """
    label_values: set[str] = set()
    for statement in statement_list:
        config = SelectorConfig(Path(), statement)
        item_values = getattr(config, label)
        label_values.update(item_values)

    return label_values


class YamlSelectors:
    """
    Parses and manages dbt YAML selector definitions from the manifest.

    This class handles the parsing of selector definitions that come from a dbt manifest file,
    converting them into syntax compatible with Cosmos node selection. The manifest provides
    selectors in a preprocessed format (dict keyed by selector name) rather than the original
    selectors.yml file format (list of selector objects).

    **Important**: This parser expects the manifest's preprocessed selector format and was NOT
    designed to support parsing selectors.yml files directly. Attempting to parse a selectors.yml
    file may result in errors.

    **Limitations**: This implementation does not support the `default` or `indirect_selection`
    properties from the dbt selector specification. It focuses on the core selection logic:
    union, intersection, and method-based selector definitions.

    Instances of this class should be created using the `parse` class method.

    :param raw_selectors: dict[str, dict[str, Any]] - The original, unparsed selector definitions from the manifest
    :param parsed_selectors: dict[str, dict[str, Any]] - The processed selector definitions converted to Cosmos format

    :property raw: dict[str, dict[str, Any]]
        The original unparsed selector definitions from the manifest.
    :property parsed: dict[str, dict[str, Any]]
        The parsed selector definitions in Cosmos format.
    :property impl_version: str
        The source code of the YamlSelectors implementation, used for change detection.

    References:
        https://docs.getdbt.com/reference/node-selection/yaml-selectors
    """

    def __init__(self, raw_selectors: dict[str, dict[str, Any]], parsed_selectors: dict[str, dict[str, Any]]):
        self._raw = raw_selectors
        self._parsed = parsed_selectors

    @property
    def raw(self) -> dict[str, dict[str, Any]]:
        """
        Get the original unparsed selector definitions.

        :return: dict[str, dict[str, Any]] - Dictionary containing the raw YAML structure
        """
        return self._raw

    @property
    def parsed(self) -> dict[str, dict[str, Any]]:
        """
        Get the parsed selector definitions in Cosmos format.

        :return: dict[str, dict[str, Any]] - Dictionary mapping selector names to their parsed definitions with select/exclude lists
        """
        return self._parsed

    @cached_property
    def impl_version(self) -> str:
        """
        Get a hash of the YamlSelectors implementation for version detection.

        This property retrieves the source code of the YamlSelectors class and returns its MD5 hash,
        which can be used to detect changes in the selector parsing logic (e.g., for cache invalidation).
        The hash is computed once and cached for the lifetime of the instance.

        :return: str - MD5 hash (32 hex characters) of the YamlSelectors class source code
        """
        source_code = inspect.getsource(self.__class__)
        return hashlib.md5(source_code.encode()).hexdigest()

    def get_raw(self, selector_name: str, default: Any = None) -> dict[str, Any] | Any:
        """
        Retrieve a raw selector definition by name.

        :param selector_name: str - Name of the selector to retrieve
        :param default: Any - Default value to return if selector is not found
        :return: dict[str, Any] | Any - The raw selector definition or the default value
        """
        return self.raw.get(selector_name, default)

    def get_parsed(self, selector_name: str, default: Any = None) -> dict[str, Any] | Any:
        """
        Retrieve a parsed selector definition by name.

        :param selector_name: str - Name of the selector to retrieve
        :param default: Any - Default value to return if selector is not found
        :return: dict[str, Any] | Any - The parsed selector definition or the default value
        """
        return self.parsed.get(selector_name, default)

    @staticmethod
    def _get_list_dicts(dct: dict[str, Any], key: str) -> list[dict[str, Any]]:
        """
        Extract and validate a list of dictionaries from a given key in a dictionary.

        This helper method retrieves a value from a dictionary and ensures it's a list containing
        either dictionaries with string keys or string values.

        :param dct: dict[str, Any] - The dictionary to extract from
        :param key: str - The key to look up in the dictionary
        :return: list[dict[str, Any]] - List of dictionaries extracted from the specified key
        :raises CosmosValueError: If the key is missing, the value is not a list, or contains invalid types
        """
        result: list[dict[str, Any]] = []
        if key not in dct:
            raise CosmosValueError(f"Expected to find key '{key}' in dict, only found {list(dct)}")
        values = dct[key]
        if not isinstance(values, list):
            raise CosmosValueError(f"Invalid value for key '{key}'. Expected a list.")
        for value in values:
            if isinstance(value, dict):
                for value_key in value:
                    if not isinstance(value_key, str):
                        raise CosmosValueError(
                            f'Expected all keys to "{key}" dict to be strings, '
                            f'but "{value_key}" is a "{type(value_key)}"'
                        )
                result.append(value)
            else:
                raise CosmosValueError(
                    f'Invalid value type {type(value)} in key "{key}", expected ' f"dict or str (value: {value})."
                )

        return result

    @staticmethod
    def _parse_selection_graph_operators(
        selector: str, parents: bool, children: bool, parents_depth: int, children_depth: int, childrens_parents: bool
    ) -> str:
        """
        Apply graph operator syntax to a selector string.

        Transforms a selector by adding graph operators (+, @) based on the specified parameters.
        These operators control traversal of the DAG to include parents, children, or both.

        :param selector: str - The base selector string to modify
        :param parents: bool - Whether to include parent nodes
        :param children: bool - Whether to include child nodes
        :param parents_depth: int - Number of parent generations to include (0 for all)
        :param children_depth: int - Number of child generations to include (0 for all)
        :param childrens_parents: bool - Whether to use the @ operator (all ancestors of children)
        :return: str - The selector string with graph operators applied
        :raises CosmosValueError: If childrens_parents is combined with depth parameters

        Examples:
            - selector="my_model", parents=True, children=False -> "+my_model"
            - selector="my_model", parents=True, children=True, parents_depth=2 -> "2+my_model+"
            - selector="my_model", childrens_parents=True -> "@my_model"
        """
        if not selector:
            return selector

        if childrens_parents and (parents_depth or children_depth):
            raise CosmosValueError("childrens_parents cannot be combined with parents_depth or children_depth.")

        if childrens_parents:
            return f"{AT_SELECTOR}{selector}"

        if parents:
            prefix = f"{parents_depth}{PLUS_SELECTOR}" if parents_depth > 0 else PLUS_SELECTOR
            selector = f"{prefix}{selector}"

        if children:
            suffix = f"{PLUS_SELECTOR}{children_depth}" if children_depth > 0 else PLUS_SELECTOR
            selector = f"{selector}{suffix}"

        return selector

    @staticmethod
    def _parse_selection_from_cosmos_spec(method: str, value: str) -> str:
        """
        Convert a dbt YAML selector method and value into Cosmos selector syntax.

        Maps dbt's method-based selector definitions to Cosmos's string-based selector format.
        For example, method="tag" and value="nightly" becomes "tag:nightly".

        :param method: str - The selector method (e.g., "tag", "path", "config.materialized")
        :param value: str - The value for the selector method
        :return: str - A Cosmos-formatted selector string
        :raises CosmosValueError: If the method is not supported

        Examples:
            - method="tag", value="nightly" -> "tag:nightly"
            - method="path", value="models/" -> "path:models/"
            - method="fqn", value="*" -> ""
            - method="config.materialized", value="view" -> "config.materialized:view"
        """
        if method == "fqn":
            return "" if value == "*" else value

        method_mappings = {
            TAG_SELECTOR[:-1]: TAG_SELECTOR,
            PATH_SELECTOR[:-1]: PATH_SELECTOR,
            SOURCE_SELECTOR[:-1]: SOURCE_SELECTOR,
            EXPOSURE_SELECTOR[:-1]: EXPOSURE_SELECTOR,
            RESOURCE_TYPE_SELECTOR[:-1]: RESOURCE_TYPE_SELECTOR,
            EXCLUDE_RESOURCE_TYPE_SELECTOR[:-1]: EXCLUDE_RESOURCE_TYPE_SELECTOR,
        }

        for method_prefix, selector_prefix in method_mappings.items():
            if method == method_prefix:
                return f"{selector_prefix}{value}"

        if any(method.startswith(f"{CONFIG_SELECTOR}{config}") for config in SUPPORTED_CONFIG):
            return f"{method}:{value}"

        raise CosmosValueError(f"Unsupported selector method: '{method}'")

    @classmethod
    def _parse_selector(cls, dct: dict[str, Any]) -> str:
        """
        Parse a single selector definition dictionary into a Cosmos selector string.

        Processes a selector definition that includes method, value, and optional graph operators
        (parents, children, etc.) and converts it to Cosmos selector syntax.

        :param dct: dict[str, Any] - Dictionary containing selector definition with keys like 'method', 'value',
                   'parents', 'children', 'parents_depth', 'children_depth', 'childrens_parents'
        :return: str - A Cosmos-formatted selector string
        :raises CosmosValueError: If depth values are not integers

        Example Input:
            {
                "method": "tag",
                "value": "nightly",
                "children": True,
                "children_depth": 2
            }

        Example Output:
            "tag:nightly+2"
        """
        method = dct["method"]
        value = dct["value"]
        parents = dct.get("parents", False)
        children = dct.get("children", False)
        parents_depth = dct.get("parents_depth", 0)
        children_depth = dct.get("children_depth", 0)
        childrens_parents = dct.get("childrens_parents", False)

        if not isinstance(parents_depth, int):
            raise CosmosValueError(f"parents_depth must be an integer, got {type(parents_depth)}: {parents_depth}")
        if not isinstance(children_depth, int):
            raise CosmosValueError(f"children_depth must be an integer, got {type(children_depth)}: {children_depth}")

        selector = cls._parse_selection_from_cosmos_spec(method, value)

        selector = cls._parse_selection_graph_operators(
            selector, parents, children, parents_depth, children_depth, childrens_parents
        )

        return selector

    @classmethod
    def _parse_exclusions(
        cls, definition: dict[str, Any], cache: dict[str, tuple[list[str], list[str]]] = {}
    ) -> list[str]:
        """
        Parse exclusion definitions from a selector definition.

        Extracts and processes 'exclude' clauses from a selector definition, recursively
        parsing any nested definitions.

        :param definition: dict[str, Any] - Dictionary containing the selector definition with an 'exclude' key
        :param cache: dict[str, tuple[list[str], list[str]]] - Cache of previously parsed selectors for reference resolution
        :return: list[str] - List of exclusion selector strings
        """
        exclusions = cls._get_list_dicts(definition, "exclude")
        exclude: list[str] = []

        for exclusion in exclusions:
            excl, _ = cls._parse_from_definition(exclusion, cache=cache)
            exclude.extend(excl)

        return exclude

    @classmethod
    def _parse_include_exclude_subdefs(
        cls,
        definitions: list[dict[str, Any]],
        cache: dict[str, tuple[list[str], list[str]]],
    ) -> tuple[list[str], list[str]]:
        """
        Parse a list of selector subdefinitions into include and exclude lists.

        Processes a list of selector definitions, separating them into inclusions and exclusions.
        Handles nested definitions and prevents multiple exclude clauses at the same level.

        :param definitions: list[dict[str, Any]] - List of selector definition dictionaries
        :param cache: dict[str, tuple[list[str], list[str]]] - Cache of previously parsed selectors for reference resolution
        :return: tuple[list[str], list[str]] - Tuple of (include_list, exclude_list) containing selector strings
        :raises CosmosValueError: If multiple exclude clauses are found at the same level
        """
        include: list[str] = []
        exclude: list[str] = []

        for definition in definitions:
            if isinstance(definition, dict) and "exclude" in definition:
                # Do not allow multiple exclude: defs at the same level
                if exclude:
                    yaml_sel_cfg = yaml.dump(definition)
                    raise CosmosValueError(
                        f"You cannot provide multiple exclude arguments to the "
                        f"same selector set operator:\n{yaml_sel_cfg}"
                    )
                definition_exclude = cls._parse_exclusions(definition, cache=cache)
                exclude.extend(definition_exclude)
            else:
                definition_include, _ = cls._parse_from_definition(definition, cache=cache)
                include.extend(definition_include)

        return (include, exclude)

    @classmethod
    def _parse_union_definition(
        cls,
        definition: dict[str, Any],
        cache: dict[str, tuple[list[str], list[str]]] = {},
    ) -> tuple[list[str], list[str]]:
        """
        Parse a union selector definition.

        A union combines multiple selectors with OR logic - nodes matching any of the
        subdefinitions will be included.

        :param definition: dict[str, Any] - Dictionary containing a 'union' key with a list of selector definitions
        :param cache: dict[str, tuple[list[str], list[str]]] - Cache of previously parsed selectors for reference resolution
        :return: tuple[list[str], list[str]] - Tuple of (include_list, exclude_list) containing selector strings

        Example Input:
            {
                "union": [
                    {"method": "tag", "value": "nightly"},
                    {"method": "path", "value": "models/staging"}
                ]
            }

        Example Output:
            (["tag:nightly", "path:models/staging"], [])
        """
        union_def_parts = cls._get_list_dicts(definition, "union")
        return cls._parse_include_exclude_subdefs(union_def_parts, cache=cache)

    @classmethod
    def _parse_intersection_definition(
        cls,
        definition: dict[str, Any],
        cache: dict[str, tuple[list[str], list[str]]],
    ) -> tuple[list[str], list[str]]:
        """
        Parse an intersection selector definition.

        An intersection combines multiple selectors with AND logic - only nodes matching all
        subdefinitions will be included. In Cosmos syntax, this is represented by joining
        selectors with commas.

        :param definition: dict[str, Any] - Dictionary containing an 'intersection' key with a list of selector definitions
        :param cache: dict[str, tuple[list[str], list[str]]] - Cache of previously parsed selectors for reference resolution
        :return: tuple[list[str], list[str]] - Tuple of (include_list, exclude_list) with intersected selectors joined by commas

        Example Input:
            {
                "intersection": [
                    {"method": "tag", "value": "nightly"},
                    {"method": "config.materialized", "value": "view"}
                ]
            }

        Example Output:
            (["tag:nightly,config.materialized:view"], [])
        """
        intersection_def_parts = cls._get_list_dicts(definition, "intersection")
        include, exclude = cls._parse_include_exclude_subdefs(intersection_def_parts, cache=cache)

        intersection = [",".join(include)]

        return (intersection, exclude)

    @classmethod
    def _parse_method_definition(
        cls,
        definition: dict[str, Any],
        cache: dict[str, tuple[list[str], list[str]]],
    ) -> tuple[list[str], list[str]]:
        """
        Parse a method-based selector definition.

        Handles selector definitions that use the 'method' and 'value' keys, including
        special handling for the 'selector' method which references other named selectors.

        :param definition: dict[str, Any] - Dictionary containing 'method' and 'value' keys, and optionally 'exclude'
        :param cache: dict[str, tuple[list[str], list[str]]] - Cache of previously parsed selectors for reference resolution
        :return: tuple[list[str], list[str]] - Tuple of (include_list, exclude_list) containing selector strings
        :raises CosmosValueError: If 'method' or 'value' keys are missing, or if a referenced
                                  selector doesn't exist

        Example Input (method-based):
            {
                "method": "tag",
                "value": "nightly",
                "exclude": [{"method": "path", "value": "models/archived"}]
            }

        Example Output:
            (["tag:nightly"], ["path:models/archived"])

        Example Input (selector reference):
            {
                "method": "selector",
                "value": "my_existing_selector"
            }

        Example Output:
            Returns the cached tuple of (include_list, exclude_list) from the referenced selector
        """
        exclude: list[str] = []

        if definition.get("method") == "selector":
            sel_def = definition.get("value")
            if sel_def not in cache:
                raise CosmosValueError(f"Existing selector definition for {sel_def} not found.")
            return cache[definition["value"]]
        elif "method" in definition and "value" in definition:
            dct = definition
            if "exclude" in definition:
                exclude = cls._parse_exclusions(definition, cache=cache)
                dct = {k: v for k, v in dct.items() if k != "exclude"}
        else:
            raise CosmosValueError(f"Expected 'method' and 'value' keys, but got {list(definition)}")

        selector = cls._parse_selector(dct)

        include = [selector] if selector else []

        return (include, exclude)

    @classmethod
    def _parse_from_definition(
        cls,
        definition: dict[str, Any],
        cache: dict[str, tuple[list[str], list[str]]],
        rootlevel: bool = False,
    ) -> tuple[list[str], list[str]]:
        """
        Recursively parse a selector definition into include and exclude lists.

        This is the main entry point for parsing selector definitions. It handles union,
        intersection, and method-based definitions, delegating to the appropriate
        specialized parsing method.

        :param definition: dict[str, Any] - The selector definition dictionary to parse
        :param cache: dict[str, tuple[list[str], list[str]]] - Cache of previously parsed selectors for reference resolution
        :param rootlevel: bool - Whether this is a root-level selector (affects validation rules)
        :return: tuple[list[str], list[str]] - Tuple of (include_list, exclude_list) containing selector strings
        :raises CosmosValueError: If the definition structure is invalid or uses unsupported formats

        Note:
            Root-level selectors can only have a single 'union' or 'intersection' key.
            CLI-style string definitions and key-value pairs are not supported - use
            method-based definitions instead.
        """
        if (
            isinstance(definition, dict)
            and ("union" in definition or "intersection" in definition)
            and rootlevel
            and len(definition) > 1
        ):
            keys = ",".join(definition.keys())
            raise CosmosValueError(
                f"Only a single 'union' or 'intersection' key is allowed "
                f"in a root level selector definition; found {keys}."
            )

        if "union" in definition:
            select, exclude = cls._parse_union_definition(definition, cache=cache)
        elif "intersection" in definition:
            select, exclude = cls._parse_intersection_definition(definition, cache=cache)
        elif "method" in definition:
            select, exclude = cls._parse_method_definition(definition, cache=cache)
        else:
            # Rejects CLI-style string definitions (e.g., definition: "tag:my_tag")
            # These should be structured as method-value definitions instead
            #
            # Rejects key-value definitions (e.g., tag: my_tag, path: models/, etc.)
            # These should be structured as method-value definitions instead
            raise CosmosValueError(
                f"Expected to find union, intersection, or method definition, instead "
                f"found {type(definition)}: {definition}"
            )

        return (select, exclude)

    @classmethod
    def parse(cls, selectors: dict[str, dict[str, Any]]) -> YamlSelectors:
        """
        Parse a complete dbt selectors YAML file into a YamlSelectors instance.

        This is the main entry point for parsing YAML selectors. It processes all selector
        definitions in the YAML file and converts them to Cosmos format.

        :param selectors: dict[str, dict[str, Any]] - The complete selectors YAML content as a dictionary with a 'selectors' key
        :return: YamlSelectors - A YamlSelectors instance containing both raw and parsed selector definitions

        Example Input:
            {
                "selectors": [
                    {
                        "name": "nightly_models",
                        "definition": {
                            "method": "tag",
                            "value": "nightly"
                        }
                    }
                ]
            }

        Example Output:
            YamlSelectors instance with parsed definitions:
            {
                "nightly_models": {
                    "select": ["tag:nightly"],
                    "exclude": None                }
            }
        """
        result: dict[str, dict[str, Any]] = {}
        cache: dict[str, tuple[list[str], list[str]]] = {}

        for name, definition in selectors.items():
            name = definition.get("name", "")
            selector_definition = definition.get("definition", {})

            select, exclude = cls._parse_from_definition(selector_definition, cache=cache, rootlevel=True)

            result[name] = {
                "select": select if select else None,
                "exclude": exclude if exclude else None,
            }

            cache[name] = (select, exclude)

        return cls(selectors, result)


def select_nodes(
    project_dir: Path | None,
    nodes: dict[str, DbtNode],
    select: list[str] | None = None,
    exclude: list[str] | None = None,
) -> dict[str, DbtNode]:
    """
    Given a group of nodes within a project, apply select and exclude filters using
    dbt node selection.

    References:
    https://docs.getdbt.com/reference/node-selection/syntax
    https://docs.getdbt.com/reference/node-selection/yaml-selectors
    """
    select = select or []
    exclude = exclude or []
    if not select and not exclude:
        return nodes
    validate_filters(exclude, select)
    subset_ids = apply_select_filter(nodes, project_dir, select)
    if select:
        nodes = get_nodes_from_subset(nodes, subset_ids)
    exclude_ids = apply_exclude_filter(nodes, project_dir, exclude)
    subset_ids = set(nodes.keys()) - exclude_ids

    return get_nodes_from_subset(nodes, subset_ids)


def get_nodes_from_subset(nodes: dict[str, DbtNode], subset_ids: set[str]) -> dict[str, DbtNode]:
    nodes = {id_: nodes[id_] for id_ in subset_ids}
    return nodes


def apply_exclude_filter(nodes: dict[str, DbtNode], project_dir: Path | None, exclude: list[str]) -> set[str]:
    exclude_ids: set[str] = set()
    for statement in exclude:
        config = SelectorConfig(project_dir, statement)
        node_selector = NodeSelector(nodes, config)
        exclude_ids.update(node_selector.select_nodes_ids_by_intersection)
    return exclude_ids


def apply_select_filter(nodes: dict[str, DbtNode], project_dir: Path | None, select: list[str]) -> set[str]:
    subset_ids: set[str] = set()
    for statement in select:
        config = SelectorConfig(project_dir, statement)
        node_selector = NodeSelector(nodes, config)
        select_ids = node_selector.select_nodes_ids_by_intersection
        subset_ids.update(select_ids)
    return subset_ids


def validate_filters(exclude: list[str], select: list[str]) -> None:
    """
    Validate select and exclude filters.
    """
    filters = [["select", select], ["exclude", exclude]]
    for filter_type, filter in filters:
        for filter_parameter in filter:
            if (
                filter_parameter.startswith(PATH_SELECTOR)
                or filter_parameter.startswith(TAG_SELECTOR)
                or filter_parameter.startswith(RESOURCE_TYPE_SELECTOR)
                or filter_parameter.startswith(EXCLUDE_RESOURCE_TYPE_SELECTOR)
                or filter_parameter.startswith(SOURCE_SELECTOR)
                or filter_parameter.startswith(EXPOSURE_SELECTOR)
                or PLUS_SELECTOR in filter_parameter
                or any([filter_parameter.startswith(CONFIG_SELECTOR + config) for config in SUPPORTED_CONFIG])
            ):
                continue
            elif ":" in filter_parameter:
                raise CosmosValueError(f"Invalid {filter_type} filter: {filter_parameter}")
