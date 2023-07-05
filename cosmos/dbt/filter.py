SUPPORTED_CONFIG = ["materialized", "schema", "tags"]


def build_paths(project_dir, selected_paths):
    return [(project_dir / path.strip("/")) for path in selected_paths]


def normalize_config(config):
    values = []
    for key, value in config.items():
        if key in SUPPORTED_CONFIG:
            if isinstance(value, list):
                normalized = [f"{key}:{item}" for item in value]
                values.extend(normalized)
            else:
                values.append(f"{key}:{value}")
    return values


def filter_nodes(project_dir, nodes, select, exclude):
    # TODO: test!
    chosen_nodes = nodes
    if select or exclude:
        chosen_nodes = nodes.copy()
        for node_id, node in nodes.items():
            # path-based filters
            paths_to_include = build_paths(project_dir, select.get("paths", []))
            paths_to_exclude = build_paths(project_dir, exclude.get("paths", []))
            node_parent_paths = node.file_path.parents
            if not set(paths_to_include).intersection(node_parent_paths) or set(paths_to_exclude).intersection(
                node_parent_paths
            ):
                chosen_nodes.pop(node_id)

            # config-based filters
            configs_to_include = normalize_config(select.get("configs", []))
            configs_to_exclude = normalize_config(exclude.get("configs", []))
            # TODO: add tags
            node_config = normalize_config(node.config)
            if not set(configs_to_include).intersection(node_config) or set(configs_to_exclude).intersection(
                node_config
            ):
                chosen_nodes.pop(node_id)
    return chosen_nodes
