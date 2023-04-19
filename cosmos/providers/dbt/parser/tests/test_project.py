from pathlib import Path

from cosmos.providers.dbt.parser.project import DbtModelType, DbtProject

DBT_PROJECT_PATH = Path("./dev/dags/dbt/")
SAMPLE_CSV_PATH = DBT_PROJECT_PATH / "jaffle_shop/seeds/raw_customers.csv"
SAMPLE_MODEL_SQL_PATH = DBT_PROJECT_PATH / "jaffle_shop/models/customers.sql"
SAMPLE_SNAPSHOT_SQL_PATH = DBT_PROJECT_PATH / "jaffle_shop/models/orders.sql"


def test_dbtproject__handle_csv_file():
    dbt_project = DbtProject(
        project_name="jaffle_shop",
    )
    assert not dbt_project.seeds
    dbt_project._handle_csv_file(SAMPLE_CSV_PATH)
    assert len(dbt_project.seeds) == 1
    assert "raw_customers" in dbt_project.seeds
    raw_customers = dbt_project.seeds["raw_customers"]
    assert raw_customers.name == "raw_customers"
    assert raw_customers.type == DbtModelType.DBT_SEED
    assert raw_customers.path == SAMPLE_CSV_PATH


def test_dbtproject__handle_sql_file_model():
    dbt_project = DbtProject(
        project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
    )
    dbt_project.models = {}

    dbt_project._handle_sql_file(SAMPLE_MODEL_SQL_PATH)
    assert len(dbt_project.models) == 1
    assert "customers" in dbt_project.models
    raw_customers = dbt_project.models["customers"]
    assert raw_customers.name == "customers"
    assert raw_customers.type == DbtModelType.DBT_MODEL
    assert raw_customers.path == SAMPLE_MODEL_SQL_PATH


def test_dbtproject__handle_sql_file_snapshot():
    dbt_project = DbtProject(
        project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
    )
    dbt_project.models = {}

    dbt_project._handle_sql_file(SAMPLE_SNAPSHOT_SQL_PATH)
    assert len(dbt_project.models) == 1
    assert "orders" in dbt_project.models
    raw_customers = dbt_project.models["orders"]
    assert raw_customers.name == "orders"
    assert raw_customers.type == DbtModelType.DBT_MODEL
    assert raw_customers.path == SAMPLE_SNAPSHOT_SQL_PATH
