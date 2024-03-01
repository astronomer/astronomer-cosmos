import pyspark.sql.functions as F


def model(dbt, session):
    stg_customers_df = dbt.ref("stg_customers")
    stg_orders_df = dbt.ref("stg_orders")
    stg_payments_df = dbt.ref("stg_payments")

    customer_orders_df = stg_orders_df.groupby("customer_id").agg(
        F.min(F.col("order_date")).alias("first_order"),
        F.max(F.col("order_date")).alias("most_recent_order"),
        F.count(F.col("order_id")).alias("number_of_orders"),
    )

    customer_payments_df = (
        stg_payments_df.join(stg_orders_df, stg_payments_df.order_id == stg_orders_df.order_id, "left")
        .groupby(stg_orders_df.customer_id)
        .agg(F.sum(F.col("amount")).alias("total_amount"))
    )

    final_df = (
        stg_customers_df.alias("customers")
        .join(
            customer_orders_df.alias("customer_orders"),
            F.col("customers.customer_id") == F.col("customer_orders.customer_id"),
            "left",
        )
        .join(
            customer_payments_df.alias("customer_payments"),
            F.col("customers.customer_id") == F.col("customer_payments.customer_id"),
            "left",
        )
        .select(
            F.col("customers.customer_id").alias("customer_id"),
            F.col("customers.first_name").alias("first_name"),
            F.col("customers.last_name").alias("last_name"),
            F.col("customer_orders.first_order").alias("first_order"),
            F.col("customer_orders.most_recent_order").alias("most_recent_order"),
            F.col("customer_orders.number_of_orders").alias("number_of_orders"),
            F.col("customer_payments.total_amount").alias("customer_lifetime_value"),
        )
    )

    return final_df
