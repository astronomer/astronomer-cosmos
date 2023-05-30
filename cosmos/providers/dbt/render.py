"""
This module contains a function to render a dbt project into Cosmos entities.
"""
import itertools
import logging

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from typing import Any, Callable, Dict, List, Optional

from airflow.exceptions import AirflowException

from cosmos.core.graph.entities import CosmosEntity, Group, Task
from cosmos.providers.dbt.core.utils.data_aware_scheduling import get_dbt_dataset
from cosmos.providers.dbt.parser.project import DbtModelType, DbtProject

logger = logging.getLogger(__name__)


def calculate_operator_class(
    execution_mode: str,
    dbt_class: str,
) -> str:
    return f"cosmos.providers.dbt.core.operators.{execution_mode}.{dbt_class}{execution_mode.capitalize()}Operator"


def render_project(
    dbt_project_name: str,
    dbt_root_path: str = "/usr/local/airflow/dags/dbt",
    dbt_models_dir: str = "models",
    dbt_snapshots_dir: str = "snapshots",
    dbt_seeds_dir: str = "seeds",
    task_args: Dict[str, Any] = {},
    operator_args: Dict[str, Any] = {},
    test_behavior: Literal["none", "after_each", "after_all"] = "after_each",
    emit_datasets: bool = True,
    conn_id: str = "default_conn_id",
    select: Dict[str, List[str]] = {},
    exclude: Dict[str, List[str]] = {},
    execution_mode: Literal["local", "docker", "kubernetes"] = "local",
    on_warning_callback: Optional[Callable] = None,
) -> Group:
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
    :param conn_id: The Airflow connection ID to use in Airflow Datasets
    :param select: A dict of dbt selector arguments (i.e., {"tags": ["tag_1", "tag_2"]})
    :param exclude: A dict of dbt exclude arguments (i.e., {"tags": ["tag_1", "tag_2]}})
    :param execution_mode: The execution mode in which the dbt project should be run.
        Options are "local", "docker", and "kubernetes".
        Defaults to "local"
     :param on_warning_callback: A callback function called on warnings with additional Context variables "test_names"
        and "test_results" of type `List`. Each index in "test_names" corresponds to the same index in "test_results".
    """
    # first, get the dbt project
    project = DbtProject(
        dbt_root_path=dbt_root_path,
        dbt_models_dir=dbt_models_dir,
        dbt_snapshots_dir=dbt_snapshots_dir,
        dbt_seeds_dir=dbt_seeds_dir,
        project_name=dbt_project_name,
    )

    # this is the group that will be returned
    base_group = Group(id=dbt_project_name)
    entities: Dict[str, CosmosEntity] = {}  # this is a dict of all the entities we create

    # add project_dir arg to task_args
    if execution_mode == "local":
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
    for model_name, model in itertools.chain(project.models.items(), project.snapshots.items(), project.seeds.items()):
        # filters down to a path within the project_dir
        if "paths" in select:
            root_directories = [project.project_dir / path.strip("/") for path in select.get("paths", [])]
            if not set(root_directories).intersection(model.path.parents):
                continue

        # filters out any specified paths
        if "paths" in exclude:
            root_directories = [project.project_dir / path.strip("/") for path in exclude.get("paths")]
            if set(root_directories).intersection(model.path.parents):
                continue

        if "configs" in select:
            # TODO: coverme
            if not set(select["configs"]).intersection(model.config.config_selectors):
                continue

        if "configs" in exclude:
            # TODO: coverme
            if set(exclude["configs"]).intersection(model.config.config_selectors):
                continue

        run_args: Dict[str, Any] = {**task_args, **operator_args, "models": model_name}
        test_args: Dict[str, Any] = {**task_args, **operator_args, "models": model_name}
        # DbtTestOperator specific arg
        test_args["on_warning_callback"] = on_warning_callback
        if emit_datasets:
            outlets = [get_dbt_dataset(conn_id, dbt_project_name, model_name)]

            if test_behavior == "after_each":
                test_args["outlets"] = outlets
            else:
                # TODO: coverme
                run_args["outlets"] = outlets

        if model.type == DbtModelType.DBT_MODEL:
            # make the run task for model
            run_task = Task(
                id=f"{model_name}_run",
                operator_class=calculate_operator_class(
                    execution_mode=execution_mode,
                    dbt_class="DbtRun",
                ),
                arguments=run_args,
            )
        elif model.type == DbtModelType.DBT_SNAPSHOT:
            # make the run task for snapshot
            run_task = Task(
                id=f"{model_name}_snapshot",
                operator_class=calculate_operator_class(
                    execution_mode=execution_mode,
                    dbt_class="DbtSnapshot",
                ),
                arguments=run_args,
            )
        elif model.type == DbtModelType.DBT_SEED:
            # make the run task for snapshot
            run_task = Task(
                id=f"{model_name}_seed",
                operator_class=calculate_operator_class(
                    execution_mode=execution_mode,
                    dbt_class="DbtSeed",
                ),
                arguments=run_args,
            )
        else:
            # TODO: coverme
            logger.error("Unknown DBT type.")
            continue

        # if test_behavior isn't "after_each", we can just add the task to the
        # base group and do nothing else for now
        if test_behavior != "after_each":
            entities[model_name] = run_task
            base_group.add_entity(entity=run_task)
            continue

        # otherwise, we need to make a test task after run tasks and turn them into a group
        entities[run_task.id] = run_task

        if model.type == DbtModelType.DBT_MODEL:
            test_task = Task(
                id=f"{model_name}_test",
                operator_class=calculate_operator_class(
                    execution_mode=execution_mode,
                    dbt_class="DbtTest",
                ),
                upstream_entity_ids=[run_task.id],
                arguments=test_args,
            )
            entities[test_task.id] = test_task
            # make the group
            model_group = Group(
                id=f"{model_name}",
                entities=[run_task, test_task],
            )
            entities[model_group.id] = model_group
            base_group.add_entity(entity=model_group)

        # all other non-run tasks don't need to be grouped with test tasks
        else:
            entities[model_name] = run_task
            base_group.add_entity(entity=run_task)

    # add dependencies now that we have all the entities
    for model_name, model in itertools.chain(project.models.items(), project.snapshots.items(), project.seeds.items()):
        upstream_deps = model.config.upstream_models
        for upstream_model_name in upstream_deps:
            try:
                dep_task = entities[upstream_model_name]
                entities[model_name].add_upstream(dep_task)
            except KeyError:
                logger.error(f"Dependency {upstream_model_name} not found for model {model}")
    if test_behavior == "after_all":
        # make a test task
        test_task = Task(
            id=f"{dbt_project_name}_test",
            operator_class=calculate_operator_class(
                execution_mode=execution_mode,
                dbt_class="DbtTest",
            ),
            arguments={**task_args, **operator_args},
        )
        entities[test_task.id] = test_task

        # add it to the base group
        base_group.add_entity(test_task)

        # add it as an upstream to all the models that don't have downstream tasks
        # since we don't have downstream info readily available, we have to iterate
        # start with all models, and remove them as we find downstream tasks
        models_with_no_downstream_tasks = [model_name for model_name, model in project.models.items()]

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
