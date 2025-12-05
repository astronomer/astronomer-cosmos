from __future__ import annotations

import os
import time
from contextlib import contextmanager
from functools import cache
from pathlib import Path
from typing import Generator

import pytest
from airflow.models.dagbag import DagBag
from dbt.version import get_installed_version as get_dbt_version
from packaging.version import Version

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
DBT_VERSION = Version(get_dbt_version().to_version_string()[1:])


@cache
def get_dag_bag() -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""
    # add everything to airflow ignore that isn't performance_dag.py
    with open(AIRFLOW_IGNORE_FILE, "w+") as f:
        for file in EXAMPLE_DAGS_DIR.iterdir():
            if file.is_file() and file.suffix == ".py":
                if file.name != "performance_dag.py":
                    print(f"Adding {file.name} to .airflowignore")
                    f.write(f"{file.name}\n")

    print(AIRFLOW_IGNORE_FILE.read_text())

    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)

    assert db.dags
    assert not db.import_errors

    return db


def generate_model_code(model_number: int) -> str:
    """
    Generates code for a dbt model with a dependency on the previous model. Runs
    a simple select statement on the previous model.
    """
    if model_number == 0:
        return f"""
        {{{{ config(materialized='table') }}}}

        select 1 as id
        """

    return f"""
    {{{{ config(materialized='table') }}}}

    select * from {{{{ ref('model_{model_number - 1}') }}}}
    """


@contextmanager
def generate_project(
    project_path: Path,
    num_models: int,
) -> Generator[Path, None, None]:
    """
    Generate dbt models in the project directory.
    """
    models_dir = project_path / "models"

    try:
        # create the models directory
        models_dir.mkdir(exist_ok=True)

        # create the models
        for i in range(num_models):
            model = models_dir / f"model_{i}.sql"
            model.write_text(generate_model_code(i))

        yield project_path
    finally:
        # clean up the models in the project_path / models directory
        for model in models_dir.iterdir():
            model.unlink()


@pytest.mark.perf
def test_perf_dag():
    num_models = os.environ.get("MODEL_COUNT", 10)

    if type(num_models) is str:
        num_models = int(num_models)

    print(f"Generating dbt project with {num_models} models")

    dbt_project_dir = EXAMPLE_DAGS_DIR / "dbt" / "perf"

    with generate_project(dbt_project_dir, num_models):
        dag_bag = get_dag_bag()

        dag = dag_bag.get_dag("performance_dag")

        # verify the integrity of the dag
        assert len(dag.tasks) == num_models

        # measure the time before and after the dag is run

        start = time.time()
        dag_run = dag.test()
        end = time.time()

        # assert the dag run was successful before writing the results
        if dag_run.state == "success":
            print(f"Ran {num_models} models in {end - start} seconds")
            print(f"NUM_MODELS={num_models}\nTIME={end - start}")

            # write the results to a file
            with open("/tmp/performance_results.txt", "w") as f:
                f.write(
                    f"NUM_MODELS={num_models}\nTIME={end - start}\nMODELS_PER_SECOND={num_models / (end - start)}\nDBT_VERSION={DBT_VERSION}"
                )
        else:
            raise Exception("Performance DAG run failed.")
