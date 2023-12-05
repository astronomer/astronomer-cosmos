from __future__ import annotations
import copy
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cosmos.constants import DbtResourceType
from cosmos.exceptions import CosmosValueError
from cosmos.log import get_logger

if TYPE_CHECKING:
    from cosmos.dbt.graph import DbtNode


SUPPORTED_CONFIG = ["materialized", "schema", "tags"]
PATH_SELECTOR = "path:"
TAG_SELECTOR = "tag:"
CONFIG_SELECTOR = "config."
PLUS_SELECTOR = "+"
GRAPH_SELECTOR_REGEX = r"^([0-9]*\+)?([^\+]+)(\+[0-9]*)?$|"

logger = get_logger(__name__)


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

    https://docs.getdbt.com/reference/node-selection/graph-operators
    """

    node_name: str
    precursors: str | None
    descendants: str | None

    @property
    def precursors_depth(self) -> int:
        """
        Calculates the depth/degrees/generations of precursors (parents).
        Return:
            -1: if it should return all the generations of precursors
            0: if it shouldn't return any precursors
            >0: upperbound number of parent generations
        """
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
            return GraphSelector(node_name, precursors, descendants)
        return None

    def select_node_precursors(self, nodes: dict[str, DbtNode], root_id: str, selected_nodes: set[str]) -> None:
        """
        Parse original nodes and add the precursor nodes related to this config to the selected_nodes set.

        :param nodes: Original dbt nodes list
        :param root_id: Unique identifier of self.node_name
        :param selected_nodes: Set where precursor nodes will be added to.
        """
        if self.precursors:
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

    def filter_nodes(self, nodes: dict[str, DbtNode]) -> set[str]:
        """
        Given a dictionary with the original dbt project nodes, applies the current graph selector to
        identify the subset of nodes that matches the selection criteria.

        :param nodes: dbt project nodes
        :return: set of node ids that matches current graph selector
        """
        selected_nodes: set[str] = set()

        # Index nodes by name, we can improve performance by doing this once
        # for multiple GraphSelectors
        node_by_name = {}
        for node_id, node in nodes.items():
            node_by_name[node.name] = node_id

        if self.node_name in node_by_name:
            root_id = node_by_name[self.node_name]
        else:
            logger.warn(f"Selector {self.node_name} not found.")
            return selected_nodes

        selected_nodes.add(root_id)
        self.select_node_precursors(nodes, root_id, selected_nodes)
        self.select_node_descendants(nodes, root_id, selected_nodes)
        return selected_nodes


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
        self.load_from_statement(statement)

    @property
    def is_empty(self) -> bool:
        return not (self.paths or self.tags or self.config or self.graph_selectors or self.other)

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
            if item.startswith(PATH_SELECTOR):
                self._parse_path_selector(item)
            elif item.startswith(TAG_SELECTOR):
                self._parse_tag_selector(item)
            elif item.startswith(CONFIG_SELECTOR):
                self._parse_config_selector(item)
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
        if key in SUPPORTED_CONFIG:
            self.config[key] = value

    def _parse_tag_selector(self, item: str) -> None:
        index = len(TAG_SELECTOR)
        self.tags.append(item[index:])

    def _parse_path_selector(self, item: str) -> None:
        index = len(PATH_SELECTOR)
        if self.project_dir:
            self.paths.append(self.project_dir / Path(item[index:]))
        else:
            self.paths.append(Path(item[index:]))

    def __repr__(self) -> str:
        return f"SelectorConfig(paths={self.paths}, tags={self.tags}, config={self.config}, other={self.other}, graph_selectors={self.graph_selectors})"


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

        for node_id, node in self.nodes.items():
            if self._should_include_node(node_id, node):
                selected_nodes.add(node_id)

        if self.config.graph_selectors:
            nodes_by_graph_selector = self.select_by_graph_operator()
            selected_nodes = selected_nodes.intersection(nodes_by_graph_selector)

        self.selected_nodes = selected_nodes
        return selected_nodes

    def _should_include_node(self, node_id: str, node: DbtNode) -> bool:
        "Checks if a single node should be included. Only runs once per node with caching."
        if node_id in self.visited_nodes:
            return node_id in self.selected_nodes

        self.visited_nodes.add(node_id)

        if node.resource_type == DbtResourceType.TEST:
            node.tags = getattr(self.nodes.get(node.depends_on[0]), "tags", [])

        if not self._is_tags_subset(node):
            return False

        node_config = {key: value for key, value in node.config.items() if key in SUPPORTED_CONFIG}

        if not self._is_config_subset(node_config):
            return False

        # Remove 'tags' as they've already been filtered for
        config_copy = copy.deepcopy(self.config.config)
        config_copy.pop("tags", None)
        node_config.pop("tags", None)

        if not (config_copy.items() <= node_config.items()):
            return False

        if self.config.paths and not self._is_path_matching(node):
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
                or PLUS_SELECTOR in filter_parameter
                or any([filter_parameter.startswith(CONFIG_SELECTOR + config + ":") for config in SUPPORTED_CONFIG])
            ):
                continue
            elif ":" in filter_parameter:
                raise CosmosValueError(f"Invalid {filter_type} filter: {filter_parameter}")
