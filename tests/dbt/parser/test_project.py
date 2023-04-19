from pathlib import Path

from cosmos.providers.dbt.parser.project import (
    DbtModel,
    DbtModelConfig,
    DbtModelType,
    DbtProject,
)


def test_dbt_model_config_selectors_add():
    """Ensure that materialized and schema are not updated once set but tags are appended."""
    sql_config = DbtModelConfig(
        config_selectors={"materialized:incremental", "tags:finance"}
    )
    schema_config = DbtModelConfig(
        config_selectors={"materialized:table", "schema:jaffle_shop", "tags:hourly"}
    )
    project_config = DbtModelConfig(
        config_selectors={
            "materialized:view",
            "schema:my_project",
            "tags:confidential",
        }
    )
    merged_config = sql_config + schema_config + project_config
    assert merged_config.config_selectors == {
        "materialized:incremental",
        "schema:jaffle_shop",
        "tags:finance",
        "tags:hourly",
        "tags:confidential",
    }


def test_dbt_model_multi_tag(tmp_path: Path):
    sql_model = tmp_path.joinpath("dummy.sql")
    sql_query = """
    {{
        config(
            materialized='incremental',
            partition_by='organisation',
            schema='jaffle_shop',
            tags=['finance', 'daily', 'confidential']
        )
    }}
    SELECT *
    FROM {{ ref('my_upstream_model') }}
    """
    sql_model.write_text(sql_query)
    dbt_model = DbtModel("jaffle_shop_revenue", DbtModelType.DBT_MODEL, sql_model)
    dbt_model_config = dbt_model.config
    assert dbt_model_config.config_selectors == {
        "materialized:incremental",
        "schema:jaffle_shop",
        "tags:finance",
        "tags:daily",
        "tags:confidential",
    }


def test_dbt_model_single_tag(tmp_path: Path):
    sql_model = tmp_path.joinpath("dummy.sql")
    sql_query = """
    {{
        config(
            materialized='incremental',
            partition_by='organisation',
            schema='jaffle_shop',
            tags='daily'
        )
    }}
    SELECT *
    FROM {{ ref('my_upstream_model') }}
    """
    sql_model.write_text(sql_query)
    dbt_model = DbtModel("jaffle_shop_revenue", DbtModelType.DBT_MODEL, sql_model)
    dbt_model_config = dbt_model.config
    assert dbt_model_config.config_selectors == {
        "materialized:incremental",
        "schema:jaffle_shop",
        "tags:daily",
    }


def test_dbt_project():
    test_project = Path(__file__).parent.parent.parent
    dbt_project = DbtProject(
        project_name="config_project", dbt_root_path=str(test_project)
    )
    models = dbt_project.models
    assert len(models) == 4
    stg_customer_config = models["stg_customer"].config
    assert stg_customer_config.config_selectors == {
        "materialized:incremental",
        "tags:hourly",
        "schema:jaffle_staging",
    }
    assert stg_customer_config.upstream_models == {"customer"}
    stg_order_config = models["stg_order"].config
    assert stg_order_config.config_selectors == {
        "materialized:incremental",
        "tags:finance",
        "schema:jaffle_staging",
    }
    customer_order_config = models["customer_order"].config
    assert customer_order_config.config_selectors == {
        "materialized:view",
        "tags:daily",
        "tags:finance",
        "tags:confidential",
    }
    assert customer_order_config.upstream_models == {"order", "customer"}
    mart_customer_config = models["mart_customer"].config
    assert mart_customer_config.config_selectors == {"tags:daily", "materialized:view"}
    assert mart_customer_config.upstream_models == {"customer_order"}
