from __future__ import annotations
from pathlib import Path


SUPPORTED_CONFIG = ["materialized", "schema", "tags"]
PATH_SELECTOR = "path:"
TAG_SELECTOR = "tag:"
CONFIG_SELECTOR = "config."


def build_paths(project_dir, selected_paths):
    return [(project_dir / path.strip("/")) for path in selected_paths]


class SelectorConfig:
    """
    Parse a select statement
    """

    def __init__(self, project_dir: Path, statement: str):
        self.project_dir = project_dir
        self.paths: list[str] = []
        self.tags: list[str] = []
        self.config: dict[str, str] = {}
        self.other: list[str] = []
        self.load_from_statement(statement)

    def load_from_statement(self, statement: str):
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
                # TODO: raise an exception or log


def select_nodes_ids_by_intersection(nodes: dict, config: SelectorConfig):
    """ """
    selected_nodes = set()
    for node_id, node in nodes.items():
        if config.tags and not (sorted(node.tags) == sorted(config.tags)):
            continue

        supported_node_config = {key: value for key, value in node.config.items() if key in SUPPORTED_CONFIG}
        if config.config and not (config.config.items() <= supported_node_config.items()):
            continue

        if config.paths and not (set(config.paths).issubset(set(node.file_path.parents))):
            continue

        selected_nodes.add(node_id)

    return selected_nodes


def select_nodes(project_dir: Path, nodes: dict, select: list[str] | None = None, exclude: list[str] | None = None):
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

    subset_ids = set()

    for statement in select:
        config = SelectorConfig(project_dir, statement)
        select_ids = select_nodes_ids_by_intersection(nodes, config)
        subset_ids = subset_ids.union(set(select_ids))

    if select:
        nodes = {id_: nodes[id_] for id_ in subset_ids}

    nodes_ids = set(nodes.keys())

    for statement in exclude:
        config = SelectorConfig(project_dir, statement)
        exclude_ids = select_nodes_ids_by_intersection(nodes, config)
        subset_ids = set(nodes_ids) - set(exclude_ids)

    return {id_: nodes[id_] for id_ in subset_ids}
