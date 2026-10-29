"""Profile the raw orders CSV before curation."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ORDERS_PATH = "data/raw/orders.csv"


def profile_orders(orders: DataFrame) -> None:
    orders = orders.cache()
    try:
        orders.printSchema()
        orders.show(10, truncate=False)

        orders.select(
            F.count("*").alias("total_rows"),
            F.count("order_id").alias("non_null_order_ids"),
            F.countDistinct("order_id").alias("distinct_order_ids"),
            F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)).alias("missing_customer_ids"),
        ).show()

        orders.select(
            F.sum(
                F.when(F.col("quantity").isNotNull() & F.expr("try_cast(quantity AS INT)").isNull(), 1).otherwise(0)
            ).alias("invalid_quantity"),
            F.sum(
                F.when(F.col("unit_price").isNotNull() & F.expr("try_cast(unit_price AS DECIMAL(12,2))").isNull(), 1).otherwise(0)
            ).alias("invalid_unit_price"),
            F.min(F.expr("try_cast(quantity AS INT)")).alias("min_quantity"),
            F.max(F.expr("try_cast(quantity AS INT)")).alias("max_quantity"),
            F.min(F.expr("try_cast(unit_price AS DECIMAL(12,2))")).alias("min_unit_price"),
            F.max(F.expr("try_cast(unit_price AS DECIMAL(12,2))")).alias("max_unit_price"),
        ).show()
    finally:
        orders.unpersist()


def main() -> None:
    spark = SparkSession.builder.appName("OrderProfiling").getOrCreate()
    try:
        orders = spark.read.option("header", True).csv(ORDERS_PATH)
        profile_orders(orders)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()