"""
This module contains a function to render a dbt project into Cosmos entities.
"""
from __future__ import annotations

import logging
import json

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from typing import Any, Callable, Dict, Optional


from cosmos.core.graph.entities import Task
from cosmos.providers.dbt.core.utils.data_aware_scheduling import get_dbt_dataset
from cosmos.providers.dbt.parser.project import DbtModel, DbtProject

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

logger = logging.getLogger(__name__)


def calculate_operator_class(
    execution_mode: str,
    dbt_class: str,
) -> str:
    "Given an execution mode and dbt class, return the operator class to use"
    return f"cosmos.providers.dbt.core.operators.{execution_mode}.{dbt_class}{execution_mode.capitalize()}Operator"


def build_map(manifest: Manifest, select: str | None) -> dict:
    """
    Iterate over the project.manifest.child_map to build a Group of all the nodes in the project
    exclude un-selected nodes
    include only models, seeds, snapshots and tests

    # TODO: deal with snapshots and seed
    """
    runner = dbtRunner()
    logger.info("Building map of dbt project", runner)
    select = select or ""
    command = (
        "ls --resource-type model snapshot seed test --output json --output-keys unique_id resource_type" + " " + select
    )
    res = runner.invoke(command.split())

    selected = {}
    results = res.result or []
    for result in results:
        result = json.loads(result)
        entity = DbtModel(**result)
        entity.node = manifest.nodes[entity.unique_id]
        selected[entity.unique_id] = entity

    models = {}
    for unique_id, children in manifest.child_map.items():
        model = selected.get(unique_id)
        if not model:
            continue
        if model.resource_type == "model":
            for child in children:
                child = selected.get(child)
                if not child:
                    continue
                if child.resource_type == "model":
                    model.child_models.append(child)
                elif child.resource_type == "test":
                    model.child_tests.append(child)
                else:
                    pass
                models[unique_id] = model
    # TODO: have specific dicts for types?
    return models


def render_project(
    dbt_project_name: str,
    dbt_root_path: str = "/usr/local/airflow/dags/dbt",
    task_args: Dict[str, Any] = {},
    operator_args: Dict[str, Any] = {},
    test_behavior: Literal["none", "after_each", "after_all"] = "after_each",
    emit_datasets: bool = True,
    conn_id: str = "default_conn_id",
    profile_args: Dict[str, str] = {},
    profile_name: str | None = None,
    target_name: str | None = None,
    select: str | None = None,
    execution_mode: Literal["local", "docker", "kubernetes"] = "local",
    on_warning_callback: Optional[Callable] = None,
) -> dict[str, DbtModel]:
    """
    Turn a dbt project into a Group

    :param dbt_project_name: The name of the dbt project
    :param dbt_root_path: The root path to your dbt folder. Defaults to /usr/local/airflow/dags/dbt
    :param task_args: Arguments to pass to the underlying dbt operators
    :param operator_args: Parameters to pass to the underlying operators, can include KubernetesPodOperator
        or DockerOperator parameters
    :param test_behavior: The behavior for running tests. Options are "none", "after_each", and "after_all".
        Defaults to "after_each"
    :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG dependencies
    :param conn_id: The Airflow connection ID to use
    :param profile_args: Arguments to pass to the dbt profile
    :param profile_name: A name to use for the dbt profile. If not provided, and no profile target is found
        in your project's dbt_project.yml, "cosmos_profile" is used.
    :param target_name: A name to use for the dbt target. If not provided, "cosmos_target" is used.
    :param select: A dbt --select, --exclude, --selector parameter string. If not provided, all models are selected.
    :param execution_mode: The execution mode in which the dbt project should be run.
        Options are "local", "docker", and "kubernetes".
        Defaults to "local"
     :param on_warning_callback: A callback function called on warnings with additional Context variables "test_names"
        and "test_results" of type `List`. Each index in "test_names" corresponds to the same index in "test_results".
    """
    # first, get the dbt project
    project = DbtProject(
        dbt_root_path=dbt_root_path,
        project_name=dbt_project_name,
    )

    logger.info("dbt project", project)
    logger.info("manifest", project.manifest)
    # add project_dir arg to task_args
    if execution_mode == "local":
        task_args["project_dir"] = project.project_dir

    # if task_args has a schema, add it to the profile args and add a deprecated warning
    if "schema" in task_args:
        profile_args["schema"] = task_args["schema"]
        logger.warning("Specifying a schema in the task_args is deprecated. Please use the profile_args instead.")

    models = build_map(project.manifest, select)

    for model in models.values():
        run_args = {
            **task_args,
            **operator_args,
            "models": model.name,
            "profile_args": profile_args,
            "profile_name": profile_name,
            "target_name": target_name,
        }
        test_args = {
            **task_args,
            **operator_args,
            "models": model.name,
            "profile_args": profile_args,
            "profile_name": profile_name,
            "target_name": target_name,
            "on_warning_callback": on_warning_callback,
        }
        # DbtTestOperator specific arg
        if emit_datasets:
            outlets = [get_dbt_dataset(conn_id, dbt_project_name, model.name)]

            if test_behavior == "after_each":
                test_args["outlets"] = outlets
            else:
                # TODO: coverme
                run_args["outlets"] = outlets

        # TODO: handle snapshots and seeds
        # make the run task for model
        model.task = Task(
            id=model.name,
            operator_class=calculate_operator_class(
                execution_mode=execution_mode,
                dbt_class="DbtRun",
            ),
            arguments=run_args,
        )

        # if test_behavior isn't "after_each", we can just add the task to the
        # base group and do nothing else for now
        # TODO: deal with this
        # if test_behavior != "after_each":
        #     entities[model_name] = run_task
        #     base_group.add_entity(entity=run_task)
        #     continue
        # otherwise, we need to make a test task after run tasks and turn them into a group
        # entities[run_task.id] = run_task

        if model.child_tests:
            for test in model.child_tests:
                test.task = Task(
                    id=test.name,
                    operator_class=calculate_operator_class(
                        execution_mode=execution_mode,
                        dbt_class="DbtTest",
                    ),
                    arguments=test_args,
                )

    return models

    # TODO: handle this
    # if test_behavior == "after_all":
    #     # make a test task
    #     test_task = Task(
    #         id=f"{dbt_project_name}_test",
    #         operator_class=calculate_operator_class(
    #             execution_mode=execution_mode,
    #             dbt_class="DbtTest",
    #         ),
    #         arguments={**task_args, **operator_args},
    #     )
    #     entities[test_task.id] = test_task
    #
    #     # add it to the base group
    #     base_group.add_entity(test_task)
    #
    #     # add it as an upstream to all the models that don't have downstream tasks
    #     # since we don't have downstream info readily available, we have to iterate
    #     # start with all models, and remove them as we find downstream tasks
    #     models_with_no_downstream_tasks = [model_name for model_name, model in project.models.items()]
    #
    #     # iterate over all models
    #     for model_name, model in project.models.items():
    #         # iterate over all upstream models
    #         for upstream_model_name in model.config.upstream_models:
    #             # remove the upstream model from the list of models with no downstream tasks
    #             try:
    #                 models_with_no_downstream_tasks.remove(upstream_model_name)
    #             except ValueError:
    #                 pass
    #
    #     # add the test task as an upstream to all models with no downstream tasks
    #     for model_name in models_with_no_downstream_tasks:
    #         if model_name in entities:
    #             test_task.add_upstream(entity=entities[model_name])
