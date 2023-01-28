"""
This module contains a function to render a dbt project into Cosmos entities.
"""
import logging

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from typing import Any, Dict, List

from airflow.datasets import Dataset
from airflow.exceptions import AirflowException

from cosmos.core.graph.entities import CosmosEntity, Group, Task
from cosmos.providers.dbt.parser.project import DbtProject

logger = logging.getLogger(__name__)


def render_project(
    dbt_project_name: str,
    dbt_root_path: str = "/usr/local/airflow/dbt",
    dbt_models_dir: str = "models",
    task_args: Dict[str, Any] = {},
    test_behavior: Literal["none", "after_each", "after_all"] = "after_each",
    emit_datasets: bool = True,
    conn_id: str = "default_conn_id",
    select: Dict[str, List[str]] = {},
    exclude: Dict[str, List[str]] = {},
) -> Group:
    """
    Turn a dbt project into a Group

    :param dbt_project_name: The name of the dbt project
    :param dbt_root_path: The root path to your dbt folder. Defaults to /usr/local/airflow/dbt
    :param task_args: Arguments to pass to the underlying dbt operators
    :param test_behavior: The behavior for running tests. Options are "none", "after_each", and "after_all".
        Defaults to "after_each"
    :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG dependencies
    :param conn_id: The Airflow connection ID to use in Airflow Datasets
    :param select: A dict of dbt selector arguments (i.e., {"tags": ["tag_1", "tag_2"]})
    :param exclude: A dict of dbt exclude arguments (i.e., {"tags": ["tag_1", "tag_2]}})
    """
    # first, get the dbt project
    project = DbtProject(
        dbt_root_path=dbt_root_path,
        dbt_models_dir=dbt_models_dir,
        project_name=dbt_project_name,
    )

    base_group = Group(id=dbt_project_name)  # this is the group that will be returned
    entities: Dict[
        str, CosmosEntity
    ] = {}  # this is a dict of all the entities we create

    # add project_dir arg to task_args
    task_args["project_dir"] = project.project_dir

    # ensures the same tag isn't in select & exclude
    if "tags" in select and "tags" in exclude:
        if set(select["tags"]).intersection(exclude["tags"]):
            raise AirflowException(
                f"Can't specify the same tag in `select` and `include`: "
                f"{set(select['tags']).intersection(exclude['tags'])}"
            )

    if "paths" in select and "paths" in exclude:
        if set(select["paths"]).intersection(exclude["paths"]):
            raise AirflowException(
                f"Can't specify the same path in `select` and `include`: "
                f"{set(select['paths']).intersection(exclude['paths'])}"
            )

    # iterate over each model once to create the initial tasks
    for model_name, model in project.models.items():
        # if we have tags, only include models that have at least one of the tags
        # filters down to a set of specified tags
        if "tags" in select:
            if not set(select["tags"]).intersection(model.config.tags):
                continue

        # filters out any specified tags
        if "tags" in exclude:
            if set(exclude["tags"]).intersection(model.config.tags):
                continue

        # filters down to a path within the project_dir
        if "paths" in select:
            root_directories = [
                project.project_dir / path.strip("/")
                for path in select.get("paths", [])
            ]
            if not set(root_directories).intersection(model.path.parents):
                continue

        # filters out any specified paths
        if "paths" in exclude:
            root_directories = [
                project.project_dir / path.strip("/") for path in exclude.get("paths")
            ]
            if set(root_directories).intersection(model.path.parents):
                continue

        run_args: Dict[str, Any] = {**task_args, "models": model_name}
        test_args: Dict[str, Any] = {**task_args, "models": model_name}

        if emit_datasets:
            outlets = [
                Dataset(
                    f"DBT://{conn_id.upper()}/{dbt_project_name.upper()}/{model_name.upper()}"
                )
            ]

            if test_behavior == "after_each":
                test_args["outlets"] = outlets
            else:
                run_args["outlets"] = outlets

        # make the run task
        run_task = Task(
            id=f"{model_name}_run",
            operator_class="cosmos.providers.dbt.core.operators.DbtRunOperator",
            arguments=run_args,
        )

        # if test_behavior isn't "after_each", we can just add the task to the
        # base group and do nothing else for now
        if test_behavior != "after_each":
            entities[model_name] = run_task
            base_group.add_entity(entity=run_task)
            continue

        # otherwise, we need to make a test task and turn them into a group
        entities[run_task.id] = run_task

        test_task = Task(
            id=f"{model_name}_test",
            operator_class="cosmos.providers.dbt.core.operators.DbtTestOperator",
            upstream_entity_ids=[run_task.id],
            arguments=test_args,
        )
        entities[test_task.id] = test_task

        # make the group
        model_group = Group(
            id=model_name,
            entities=[run_task, test_task],
        )
        entities[model_group.id] = model_group

        # just add to base group for now
        base_group.add_entity(entity=model_group)

    # add dependencies now that we have all the entities
    for model_name, model in project.models.items():
        upstream_deps = model.config.upstream_models
        for upstream_model_name in upstream_deps:
            try:
                dep_task = entities[upstream_model_name]
                entities[model_name].add_upstream(dep_task)
            except KeyError:
                logger.error(
                    f"Dependency {upstream_model_name} not found for model {model}"
                )

    if test_behavior == "after_all":
        # make a test task
        test_task = Task(
            id=f"{dbt_project_name}_test",
            operator_class="cosmos.providers.dbt.core.operators.DbtTestOperator",
            arguments=task_args,
        )
        entities[test_task.id] = test_task

        # add it to the base group
        base_group.add_entity(test_task)

        # add it as an upstream to all the models that don't have downstream tasks
        # since we don't have downstream info readily available, we have to iterate
        # start with all models, and remove them as we find downstream tasks
        models_with_no_downstream_tasks = [
            model_name for model_name, model in project.models.items()
        ]

        # iterate over all models
        for model_name, model in project.models.items():
            # iterate over all upstream models
            for upstream_model_name in model.config.upstream_models:
                # remove the upstream model from the list of models with no downstream tasks
                try:
                    models_with_no_downstream_tasks.remove(upstream_model_name)
                except ValueError:
                    pass

        # add the test task as an upstream to all models with no downstream tasks
        for model_name in models_with_no_downstream_tasks:
            if model_name in entities:
                test_task.add_upstream(entity=entities[model_name])

    return base_group
