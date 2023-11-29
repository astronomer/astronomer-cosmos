from __future__ import annotations
import copy
import re
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
    node_name: str
    precursors: str | None
    descendants: str | None

    @property
    def precursors_depth(self) -> int:
        if not self.precursors:
            return 0
        if self.precursors == "+":
            return -1
        else:
            return int(self.precursors[:-1])

    @property
    def descendants_depth(self) -> int:
        if not self.descendants:
            return 0
        if self.descendants == "+":
            return -1
        else:
            return int(self.descendants[1:])

    @staticmethod
    def parse(text: str) -> GraphSelector | None:
        regex_match = re.search(GRAPH_SELECTOR_REGEX, text)
        if regex_match:
            precursors, node_name, descendants = regex_match.groups()
            return GraphSelector(node_name, precursors, descendants)
        return None


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
        return not (self.paths or self.tags or self.config or self.other)

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
                index = len(PATH_SELECTOR)
                if self.project_dir:
                    self.paths.append(self.project_dir / Path(item[index:]))
                else:
                    self.paths.append(Path(item[index:]))
            elif item.startswith(TAG_SELECTOR):
                index = len(TAG_SELECTOR)
                self.tags.append(item[index:])
            elif item.startswith(CONFIG_SELECTOR):
                index = len(CONFIG_SELECTOR)
                key, value = item[index:].split(":")
                if key in SUPPORTED_CONFIG:
                    self.config[key] = value
            else:
                graph_selector = GraphSelector.parse(item)
                if graph_selector is not None:
                    self.graph_selectors.append(graph_selector)
                else:
                    self.other.append(item)
                    logger.warning("Unsupported select statement: %s", item)

    def __repr__(self) -> str:
        return f"SelectorConfig(paths={self.paths}, tags={self.tags}, config={self.config}, other={self.other})"


class NodeSelector:
    """
    Class to select nodes based on a selector config.

    :param nodes: Dictionary mapping dbt nodes (node.unique_id to node)
    :param config: User-defined select statements
    """

    def __init__(self, nodes: dict[str, DbtNode], config: SelectorConfig) -> None:
        self.nodes = nodes
        self.config = config

    def select_nodes_ids_by_intersection(self) -> set[str]:
        """
        Return a list of node ids which matches the configuration defined in config.

        References:
        https://docs.getdbt.com/reference/node-selection/syntax
        https://docs.getdbt.com/reference/node-selection/yaml-selectors
        """
        if self.config.is_empty:
            return set(self.nodes.keys())

        self.selected_nodes: set[str] = set()
        self.visited_nodes: set[str] = set()

        for node_id, node in self.nodes.items():
            if self._should_include_node(node_id, node):
                self.selected_nodes.add(node_id)

        return self.selected_nodes

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

    def select_node_precursors(self) -> set[str]:
        """
        Return a list of node ids which match the configuration defined in the config.

        Return all nodes that are parents (or parents from parents) of the root defined in the configuration.

        References:
        https://docs.getdbt.com/reference/node-selection/syntax
        https://docs.getdbt.com/reference/node-selection/yaml-selectors
        """
        selected_nodes = set()

        # Index nodes by name
        node_by_name = {}
        for node_id, node in self.nodes.items():
            node_by_name[node.name] = node_id

        for graph_selector in self.config.graph_selectors:
            if graph_selector.node_name in node_by_name:
                root_id = node_by_name[graph_selector.node_name]
            else:
                logger.warn(f"Selector {graph_selector.node_name} not found.")
                break

            selected_nodes.add(root_id)

            if graph_selector.precursors:
                depth = graph_selector.precursors_depth
                previous_generation = {root_id}
                processed_nodes = set()
                while depth and previous_generation:
                    new_generation: set[str] = set()
                    for node_id in previous_generation:
                        if node_id not in processed_nodes:
                            new_generation = new_generation.union(set(self.nodes[node_id].depends_on))
                            processed_nodes.add(node_id)
                    selected_nodes = selected_nodes.union(new_generation)
                    previous_generation = new_generation
                    depth -= 1

        return selected_nodes


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
        label_values = label_values.union(item_values)

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

    # validates select and exclude filters
    filters = [["select", select], ["exclude", exclude]]
    for filter_type, filter in filters:
        for filter_parameter in filter:
            if (
                filter_parameter.startswith(PATH_SELECTOR)
                or filter_parameter.startswith(TAG_SELECTOR)
                or PLUS_SELECTOR in filter_parameter
            ):
                continue
            elif any([filter_parameter.startswith(CONFIG_SELECTOR + config + ":") for config in SUPPORTED_CONFIG]):
                continue
            elif ":" in filter_parameter:
                raise CosmosValueError(f"Invalid {filter_type} filter: {filter_parameter}")
            else:
                logger.warn(f"Best effort in processing filter {filter}")

    subset_ids: set[str] = set()

    for statement in select:
        config = SelectorConfig(project_dir, statement)
        node_selector = NodeSelector(nodes, config)
        # TODO: Fix this
        if config.graph_selectors:
            select_ids = node_selector.select_node_precursors()
            subset_ids = subset_ids.union(set(select_ids))
        else:
            select_ids = node_selector.select_nodes_ids_by_intersection()
            subset_ids = subset_ids.union(set(select_ids))

    if select:
        nodes = {id_: nodes[id_] for id_ in subset_ids}

    nodes_ids = set(nodes.keys())

    exclude_ids: set[str] = set()
    for statement in exclude:
        config = SelectorConfig(project_dir, statement)
        node_selector = NodeSelector(nodes, config)
        exclude_ids = exclude_ids.union(set(node_selector.select_nodes_ids_by_intersection()))
        subset_ids = set(nodes_ids) - set(exclude_ids)

    return {id_: nodes[id_] for id_ in subset_ids}
