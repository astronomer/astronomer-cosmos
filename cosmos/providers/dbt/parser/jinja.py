from typing import Dict

import os
from pathlib import Path

import jinja2
import yaml


def extract_model_data(project_path: str) -> Dict[str, Dict[str, list]]:
    """
    Takes a path to a dbt project and returns a list of all the
    models with their dependencies. Return value is a dictionary
    with the model name as the key and a list of dependencies as
    the value.

    Example:
    {
        "model_a": ["model_b", "model_c"],
        "model_b": ["model_c"],
        "model_c": [],
    }

    :param project_path: The path to the dbt project.
    :type project_path: str
    """
    models_path = os.path.join(project_path, "models")

    if not os.path.exists(models_path):
        raise ValueError(f"models_path {models_path} does not exist.")

    if not os.path.isdir(models_path):
        raise ValueError(f"models_path {models_path} is not a directory.")

    # get all the files recursively
    all_files = [os.path.join(root, f) for root, _, files in os.walk(models_path) for f in files]

    model_data = {}

    # MODELS (SQL Files)
    model_files = [path for path in all_files if os.path.isfile(path) and path.endswith(".sql")]

    for model_file in model_files:
        with open(model_file, encoding="utf-8") as f:
            model = f.read()

        env = jinja2.Environment()
        ast = env.parse(model)

        # if there's a dependency, add it to the list
        model_deps = []
        tags = []
        for base_node in ast.find_all(jinja2.nodes.Call):
            if hasattr(base_node.node, "name"):
                if base_node.node.name == "ref":
                    model_deps.append(base_node.args[0].value)
                if base_node.node.name == "config" and base_node.kwargs[0].key == "tags":
                    lst = base_node.kwargs[0].value
                    tags = [x.value for x in lst.items if lst.items]

        # add the dependencies to the dictionary, without the .sql extension
        # add the tags to the dictionary
        model_data[Path(model_file).stem] = {"dependencies": model_deps, "tags": tags}

    # PROPERTIES.YML
    properties_files = [path for path in all_files if os.path.isfile(path) and path.endswith(".yml")]

    # get tags for models from property files
    for properties_file in properties_files:
        with open(properties_file) as f:
            properties = yaml.safe_load(f)

        models = properties["models"]
        for model in models:
            properties_tags = []

            if "config" in model:
                config = model["config"]
                [properties_tags.append(item) for item in config.get("tags", [])]
                [properties_tags.append(item) for item in config.get("+tags", [])]

            if model.get("name") in model_data:
                # add tags to model_data without deleting the tags collected from models
                exiting_tags = model_data[model.get("name")]["tags"]
                model_data[model.get("name")]["tags"] = list(set(exiting_tags).union(set(properties_tags)))

    # TODO: Decide if we want to include tags from the project file

    # DBT_PROJECT.YML
    # project_file = os.path.join(project_path, "dbt_project.yml")

    return model_data
