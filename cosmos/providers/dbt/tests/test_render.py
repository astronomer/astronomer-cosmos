from pathlib import Path

import pytest
from airflow.exceptions import AirflowException

from cosmos.providers.dbt.render import calculate_operator_class, render_project

DBT_PROJECT_PATH = Path(__name__).parent.parent.parent.parent.parent / "dev/dags/dbt/"


def test_calculate_operator_class():
    class_module_import_path = calculate_operator_class(
        execution_mode="kubernetes", dbt_class="Seed"
    )
    assert (
        class_module_import_path
        == "cosmos.providers.dbt.core.operators.kubernetes.SeedKubernetesOperator"
    )


def test_render_project():
    render_project(dbt_project_name="jaffle_shop", dbt_root_path=DBT_PROJECT_PATH)


def test_render_project_select_and_exclude_conflict():
    with pytest.raises(AirflowException) as exc_info:
        render_project(
            dbt_project_name="jaffle_shop",
            dbt_root_path=DBT_PROJECT_PATH,
            select={"tags": ["tag_1"]},
            exclude={"tags": ["tag_1"]},
        )
    computed = exc_info.value.args[0]
    expected = "Can't specify the same tag in `select` and `include`: {'tag_1'}"
    assert computed == expected
