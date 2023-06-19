from __future__ import annotations


from cosmos.providers.dbt.parser.project import DbtModel, DbtProject
from cosmos.providers.dbt.render import build_map

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest

from tests import PROJECT_ROOT

DBT_PROJECT_PATH = PROJECT_ROOT / "dev/dags/dbt/"
SAMPLE_CSV_PATH = DBT_PROJECT_PATH / "jaffle_shop/seeds/raw_customers.csv"
SAMPLE_MODEL_SQL_PATH = DBT_PROJECT_PATH / "jaffle_shop/models/customers.sql"
SAMPLE_SNAPSHOT_SQL_PATH = DBT_PROJECT_PATH / "jaffle_shop/models/orders.sql"


def test_dbtproject__build_map():
    runner = dbtRunner()
    res: dbtRunnerResult = runner.invoke(["parse"])
    manifest = res.result
    select = "--select stg_payments+ --exclude customers"
    models = build_map(manifest, select)
    orders = DbtModel("model.jaffle_shop.orders", "model")
    orders.child_tests.append(
        [
            DbtModel(
                "test.jaffle_shop.accepted_values_orders_status__placed__shipped__completed__return_pending__returned.be6b5b5ec3",
                "test",
            ),
            DbtModel("test.jaffle_shop.not_null_orders_amount.106140f9fd", "test"),
            DbtModel("test.jaffle_shop.not_null_orders_bank_transfer_amount.7743500c49", "test"),
            DbtModel("test.jaffle_shop.not_null_orders_coupon_amount.ab90c90625", "test"),
            DbtModel("test.jaffle_shop.not_null_orders_credit_card_amount.d3ca593b59", "test"),
            DbtModel("test.jaffle_shop.not_null_orders_customer_id.c5f02694af", "test"),
            DbtModel("test.jaffle_shop.not_null_orders_gift_card_amount.413a0d2d7a", "test"),
            DbtModel("test.jaffle_shop.not_null_orders_order_id.cf6c17daed", "test"),
            DbtModel("test.jaffle_shop.unique_orders_order_id.fed79b3a6e", "test"),
        ]
    )
    payments = DbtModel("model.jaffle_shop.stg_payments", "model")
    payments.child_models.append([DbtModel("model.jaffle_shop.orders", "model")])
    payments.child_tests.append(
        [
            DbtModel(
                "test.jaffle_shop.accepted_values_stg_payments_payment_method__credit_card__coupon__bank_transfer__gift_card.3c3820f278",
                "test",
            ),
            DbtModel("test.jaffle_shop.not_null_stg_payments_payment_id.c19cc50075", "test"),
            DbtModel("test.jaffle_shop.unique_stg_payments_payment_id.3744510712", "test"),
        ]
    )
    assert models == {
        orders.unique_id: orders,
        payments.unique_id: payments,
    }


def test_dbtproject__loads_manifest():
    dbt_project = DbtProject(
        project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
    )
    assert isinstance(dbt_project.manifest, Manifest)
