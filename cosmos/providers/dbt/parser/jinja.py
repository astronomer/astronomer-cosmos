import os
from typing import Dict, List
from pathlib import Path

import jinja2

def extract_deps_from_models(project_path: str) -> Dict[str, List[str]]:
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
    all_files = [
        os.path.join(root, f)
        for root, _, files in os.walk(models_path)
        for f in files
    ]

    # get all the model files
    model_files = [
        path for path in all_files
        if os.path.isfile(path) and path.endswith(".sql")
    ]


    dependencies = {}

    for model_file in model_files:
        with open(model_file, "r", encoding="utf-8") as f:
            model = f.read()

        env = jinja2.Environment()
        ast = env.parse(model)

        # if there's a dependency, add it to the list
        model_deps = []
        for base_node in ast.find_all(jinja2.nodes.Call):
            if hasattr(base_node.node, "name"):
                if base_node.node.name == "ref":
                    model_deps.append(base_node.args[0].value)

        # add the dependencies to the dictionary, without the .sql extension
        dependencies[Path(model_file).stem] = model_deps

    return dependencies