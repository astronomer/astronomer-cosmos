import pyspark.sql.functions as F


def model(dbt, session):
    stg_orders_df = dbt.ref("stg_orders")
    stg_payments_df = dbt.ref("stg_payments")

    payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]

    agg_list = [
        F.sum(F.when(stg_payments_df.payment_method == payment_method, stg_payments_df.amount).otherwise(0)).alias(
            payment_method + "_amount"
        )
        for payment_method in payment_methods
    ]
    agg_list.append(F.sum(F.col("amount")).alias("total_amount"))

    order_payments_df = stg_payments_df.groupby("order_id").agg(*agg_list)

    final_df = stg_orders_df.join(
        order_payments_df, stg_orders_df.order_id == order_payments_df.order_id, "left"
    ).select(
        stg_orders_df.order_id.alias("order_id"),
        stg_orders_df.customer_id.alias("customer_id"),
        stg_orders_df.order_date.alias("order_date"),
        stg_orders_df.status.alias("status"),
        *[F.col(payment_method + "_amount") for payment_method in payment_methods],
        order_payments_df.total_amount.alias("amount"),
    )

    return final_df
