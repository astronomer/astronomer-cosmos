from __future__ import annotations
from pathlib import Path

from typing import TYPE_CHECKING

from cosmos.exceptions import CosmosValueError
from cosmos.log import get_logger

if TYPE_CHECKING:
    from cosmos.dbt.graph import DbtNode


SUPPORTED_CONFIG = ["materialized", "schema", "tags"]
PATH_SELECTOR = "path:"
TAG_SELECTOR = "tag:"
CONFIG_SELECTOR = "config."


logger = get_logger(__name__)


class SelectorConfig:
    """
    Represents a select/exclude statement.
    Supports to load it from a string.
    """

    def __init__(self, project_dir: Path, statement: str):
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
        self.load_from_statement(statement)
        self.is_config_empty: bool = False

        if not self.paths and not self.tags and not self.config and not self.other:
            self.is_config_empty = True

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
                self.paths.append(self.project_dir / item[index:])
            elif item.startswith(TAG_SELECTOR):
                index = len(TAG_SELECTOR)
                self.tags.append(item[index:])
            elif item.startswith(CONFIG_SELECTOR):
                index = len(CONFIG_SELECTOR)
                key, value = item[index:].split(":")
                if key in SUPPORTED_CONFIG:
                    self.config[key] = value
            else:
                self.other.append(item)
                logger.warning("Unsupported select statement: %s", item)


def select_nodes_ids_by_intersection(nodes: dict[str, DbtNode], config: SelectorConfig) -> set[str]:
    """
    Return a list of node ids which matches the configuration defined in config.

    :param nodes: Dictionary mapping dbt nodes (node.unique_id to node)
    :param config: User-defined select statements

    References:
    https://docs.getdbt.com/reference/node-selection/syntax
    https://docs.getdbt.com/reference/node-selection/yaml-selectors
    """
    selected_nodes = set()
    for node_id, node in nodes.items():
        if config.is_config_empty:
            continue

        if config.tags and not (sorted(node.tags) == sorted(config.tags)):
            continue

        supported_node_config = {key: value for key, value in node.config.items() if key in SUPPORTED_CONFIG}
        if config.config:
            config_tag = config.config.get("tags")
            if config_tag and config_tag not in supported_node_config.get("tags", []):
                continue

            # Remove 'tags' as they've already been filtered for
            config.config.pop("tags", None)
            supported_node_config.pop("tags", None)

            if not (config.config.items() <= supported_node_config.items()):
                continue

        if config.paths and not (set(config.paths).issubset(set(node.file_path.parents))):
            continue

        selected_nodes.add(node_id)

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
    project_dir: Path, nodes: dict[str, DbtNode], select: list[str] | None = None, exclude: list[str] | None = None
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
            if filter_parameter.startswith(PATH_SELECTOR) or filter_parameter.startswith(TAG_SELECTOR):
                continue
            elif any([filter_parameter.startswith(CONFIG_SELECTOR + config + ":") for config in SUPPORTED_CONFIG]):
                continue
            else:
                raise CosmosValueError(f"Invalid {filter_type} filter: {filter_parameter}")

    subset_ids: set[str] = set()

    for statement in select:
        config = SelectorConfig(project_dir, statement)
        select_ids = select_nodes_ids_by_intersection(nodes, config)
        subset_ids = subset_ids.union(set(select_ids))

    if select:
        nodes = {id_: nodes[id_] for id_ in subset_ids}

    nodes_ids = set(nodes.keys())

    exclude_ids: set[str] = set()
    for statement in exclude:
        config = SelectorConfig(project_dir, statement)
        exclude_ids = exclude_ids.union(set(select_nodes_ids_by_intersection(nodes, config)))
        subset_ids = set(nodes_ids) - set(exclude_ids)

    return {id_: nodes[id_] for id_ in subset_ids}
