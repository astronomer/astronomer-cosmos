from pathlib import Path

import pytest
from airflow.exceptions import AirflowException

from cosmos.core.graph.entities import Group
from cosmos.providers.dbt.render import calculate_operator_class, render_project

DBT_PROJECT_PATH = Path(__name__).parent.parent.parent.parent.parent / "dev/dags/dbt/"


def test_calculate_operator_class():
    class_module_import_path = calculate_operator_class(execution_mode="kubernetes", dbt_class="Seed")
    assert class_module_import_path == "cosmos.providers.dbt.core.operators.kubernetes.SeedKubernetesOperator"


def test_render_project_default():
    computed = render_project(dbt_project_name="jaffle_shop", dbt_root_path=DBT_PROJECT_PATH)
    assert isinstance(computed, Group)
    assert computed.id == "jaffle_shop"
    assert len(computed.entities) == 8
    entities_ids = [entity.id for entity in computed.entities]
    expected_ids = [
        "customers",
        "orders",
        "raw_customers_seed",
        "raw_orders_seed",
        "raw_payments_seed",
        "stg_customers",
        "stg_orders",
        "stg_payments",
    ]
    assert sorted(entities_ids) == expected_ids


def test_render_project_test_behavior_none():
    computed = render_project(
        dbt_project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
        test_behavior="none",
    )
    assert isinstance(computed, Group)
    assert computed.id == "jaffle_shop"
    assert len(computed.entities) == 8
    entities_ids = [entity.id for entity in computed.entities]
    expected_ids = [
        "customers_run",
        "orders_run",
        "raw_customers_seed",
        "raw_orders_seed",
        "raw_payments_seed",
        "stg_customers_run",
        "stg_orders_run",
        "stg_payments_run",
    ]
    assert sorted(entities_ids) == expected_ids


def test_render_project_test_behavior_after_all():
    computed = render_project(
        dbt_project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
        test_behavior="after_all",
    )
    assert isinstance(computed, Group)
    assert computed.id == "jaffle_shop"
    assert len(computed.entities) == 9
    entities_ids = [entity.id for entity in computed.entities]
    expected_ids = [
        "customers_run",
        "jaffle_shop_test",
        "orders_run",
        "raw_customers_seed",
        "raw_orders_seed",
        "raw_payments_seed",
        "stg_customers_run",
        "stg_orders_run",
        "stg_payments_run",
    ]
    assert sorted(entities_ids) == expected_ids


@pytest.mark.parametrize(
    "selectors,error_message",
    [
        (
            {"tags": ["tag_1"]},
            "Can't specify the same tag in `select` and `include`: {'tag_1'}",
        ),
        (
            {"paths": ["/tmp"]},
            "Can't specify the same path in `select` and `include`: {'/tmp'}",
        ),
    ],
    ids=["tags", "paths"],
)
def test_render_project_select_and_exclude_conflict(selectors, error_message):
    with pytest.raises(AirflowException) as exc_info:
        render_project(
            dbt_project_name="jaffle_shop",
            dbt_root_path=DBT_PROJECT_PATH,
            select=selectors,
            exclude=selectors,
        )
    computed = exc_info.value.args[0]
    assert computed == error_message


def test_render_project_select_models_by_including_path():
    computed = render_project(
        dbt_project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
        select={"paths": ["models/staging/"]},
    )
    assert len(computed.entities) == 3
    entities_ids = [entity.id for entity in computed.entities]
    expected_ids = ["stg_customers", "stg_orders", "stg_payments"]
    assert sorted(entities_ids) == expected_ids


def test_render_project_select_models_by_excluding_path():
    computed = render_project(
        dbt_project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
        exclude={"paths": ["models/staging/"]},
    )
    assert len(computed.entities) == 5
    entities_ids = [entity.id for entity in computed.entities]
    expected_ids = [
        "customers",
        "orders",
        "raw_customers_seed",
        "raw_orders_seed",
        "raw_payments_seed",
    ]
    assert sorted(entities_ids) == expected_ids
