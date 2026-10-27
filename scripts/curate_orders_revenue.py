"""Curate completed-order revenue by customer with PySpark.

Stages: validate columns -> clean and type -> classify rows -> keep valid completed orders
-> revenue -> aggregate by customer -> left join to customers -> write Parquet.

Rows that fail any check are written to rejected_* outputs with their reasons.
Nothing is dropped silently, and reconciliation counts are checked before writing.

Duplicate rule: an order_id that appears more than once is quarantined in full
(every row of the group goes to rejected_orders). No row is chosen as the survivor,
because the source rule for that choice is not defined yet.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

LOGGER = logging.getLogger("curate_orders_revenue")

CUSTOMER_COLUMNS = ["customer_id", "customer_name", "region"]
ORDER_COLUMNS = ["order_id", "customer_id", "quantity", "unit_price", "status"]
ACCEPTED_STATUSES = ["COMPLETED", "CANCELLED", "PENDING"]
COMPLETED = "COMPLETED"

ID_PATTERN = r"^[0-9]+$"
INT_PATTERN = r"^-?[0-9]+$"
PRICE_PATTERN = r"^-?[0-9]+(\.[0-9]{1,2})?$"

REVENUE_TYPE = "decimal(18,2)"


@dataclass(frozen=True)
class CurationResult:
    customer_revenue: DataFrame
    completed_orders: DataFrame
    rejected_orders: DataFrame
    rejected_customers: DataFrame
    counts: Dict[str, Any]


def validate_columns(df: DataFrame, required: List[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def _clean(df: DataFrame) -> DataFrame:
    """Trim strings and turn blank values into null."""
    return df.select(
        [
            F.when(F.trim(F.col(c).cast("string")) == "", None)
            .otherwise(F.trim(F.col(c).cast("string")))
            .alias(c)
            for c in df.columns
        ]
    )


def _parse(col_name: str, pattern: str, sql_type: str) -> Column:
    """Typed value if the text matches the pattern and casts cleanly, else null.

    The regex blocks silent truncation (e.g. "2.5" -> 2 as INT). try_cast avoids
    ANSI-mode errors on bad input.
    """
    return F.when(
        F.col(col_name).rlike(pattern),
        F.expr(f"try_cast({col_name} AS {sql_type})"),
    )


def _reasons(checks: List[Tuple[str, Column]]) -> Column:
    """Array of reason codes for every failed check."""
    return F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in checks]))


def _field_checks(raw_col: str, typed_col: str, label: str) -> List[Tuple[str, Column]]:
    return [
        (f"missing_{label}", F.col(raw_col).isNull()),
        (f"invalid_{label}", F.col(raw_col).isNotNull() & F.col(typed_col).isNull()),
    ]


def _split_reasons(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    rejected = df.filter(F.size("reasons") > 0)
    valid = df.filter(F.size("reasons") == 0)
    return valid, rejected


def _reason_counts(rejected: DataFrame) -> Dict[str, int]:
    rows = rejected.select(F.explode("reasons").alias("reason")).groupBy("reason").count().collect()
    return {r["reason"]: r["count"] for r in rows}


def _classify_customers(customers_raw: DataFrame) -> Tuple[DataFrame, DataFrame]:
    cust = _clean(customers_raw.select(*CUSTOMER_COLUMNS))
    cust = cust.select(
        F.col("customer_id").alias("customer_id_raw"),
        _parse("customer_id", ID_PATTERN, "BIGINT").alias("customer_id"),
        "customer_name",
        "region",
    )
    dup_count = F.when(
        F.col("customer_id").isNotNull(),
        F.count(F.lit(1)).over(Window.partitionBy("customer_id")),
    )
    cust = cust.withColumn("customer_dup_count", dup_count)
    flagged = cust.withColumn(
        "reasons",
        _reasons(
            _field_checks("customer_id_raw", "customer_id", "customer_id")
            + [("duplicate_customer_id", F.col("customer_dup_count") > 1)]
        ),
    ).cache()
    valid, rejected = _split_reasons(flagged)
    return valid.select("customer_id", "customer_name", "region"), rejected


def _classify_orders(orders_raw: DataFrame, known_customer_ids: DataFrame) -> DataFrame:
    ords = _clean(orders_raw.select(*ORDER_COLUMNS))
    typed = ords.select(
        F.col("order_id").alias("order_id_raw"),
        F.col("customer_id").alias("customer_id_raw"),
        F.col("quantity").alias("quantity_raw"),
        F.col("unit_price").alias("unit_price_raw"),
        F.col("status").alias("status_raw"),
        _parse("order_id", ID_PATTERN, "BIGINT").alias("order_id"),
        _parse("customer_id", ID_PATTERN, "BIGINT").alias("customer_id"),
        _parse("quantity", INT_PATTERN, "INT").alias("quantity"),
        _parse("unit_price", PRICE_PATTERN, "DECIMAL(12,2)").alias("unit_price"),
        F.upper(F.col("status")).alias("status"),
    )

    known = known_customer_ids.withColumn("customer_known", F.lit(True))
    typed = typed.join(F.broadcast(known), "customer_id", "left")

    dup_count = F.when(
        F.col("order_id").isNotNull(),
        F.count(F.lit(1)).over(Window.partitionBy("order_id")),
    )
    typed = typed.withColumn("order_dup_count", dup_count)

    checks = (
        _field_checks("order_id_raw", "order_id", "order_id")
        + _field_checks("customer_id_raw", "customer_id", "customer_id")
        + _field_checks("quantity_raw", "quantity", "quantity")
        + _field_checks("unit_price_raw", "unit_price", "unit_price")
        + [
            ("missing_status", F.col("status_raw").isNull()),
            (
                "invalid_status",
                F.col("status").isNotNull() & ~F.col("status").isin(ACCEPTED_STATUSES),
            ),
            ("quantity_not_positive", F.col("quantity") <= 0),
            ("unit_price_negative", F.col("unit_price") < 0),
            ("duplicate_order_id", F.col("order_dup_count") > 1),
            (
                "unknown_customer_id",
                F.col("customer_id").isNotNull() & F.col("customer_known").isNull(),
            ),
        ]
    )
    return typed.withColumn("reasons", _reasons(checks)).cache()


def curate(customers_raw: DataFrame, orders_raw: DataFrame) -> CurationResult:
    """Run the transformation on raw (string-typed or typed) inputs. Pure function, no I/O."""
    validate_columns(customers_raw, CUSTOMER_COLUMNS, "customers")
    validate_columns(orders_raw, ORDER_COLUMNS, "orders")

    customers_ok, rejected_customers = _classify_customers(customers_raw)
    flagged = _classify_orders(orders_raw, customers_ok.select("customer_id"))

    valid_orders, rejected_orders = _split_reasons(flagged)
    completed = valid_orders.filter(F.col("status") == COMPLETED)
    not_completed = valid_orders.filter(F.col("status") != COMPLETED)

    orders_in = flagged.count()
    orders_rejected = rejected_orders.count()
    orders_completed = completed.count()
    orders_not_completed = not_completed.count()
    if orders_in != orders_rejected + orders_completed + orders_not_completed:
        raise RuntimeError(
            "Order reconciliation failed: "
            f"in={orders_in} rejected={orders_rejected} "
            f"completed={orders_completed} not_completed={orders_not_completed}"
        )

    customers_in = customers_ok.count() + rejected_customers.count()
    customers_total = customers_raw.count()
    if customers_in != customers_total:
        raise RuntimeError(f"Customer reconciliation failed: in={customers_total} out={customers_in}")

    completed = completed.withColumn(
        "revenue", (F.col("quantity") * F.col("unit_price")).cast(REVENUE_TYPE)
    )
    per_customer = completed.groupBy("customer_id").agg(
        F.count(F.lit(1)).cast("long").alias("completed_order_count"),
        F.sum("revenue").cast(REVENUE_TYPE).alias("total_revenue"),
    )
    customer_revenue = (
        customers_ok.join(per_customer, "customer_id", "left")
        .select(
            "customer_id",
            "customer_name",
            "region",
            F.coalesce(F.col("completed_order_count"), F.lit(0)).cast("long").alias("completed_order_count"),
            F.coalesce(F.col("total_revenue"), F.lit(0)).cast(REVENUE_TYPE).alias("total_revenue"),
        )
    )

    counts = {
        "customers_in": customers_total,
        "customers_valid": customers_ok.count(),
        "customers_rejected": rejected_customers.count(),
        "orders_in": orders_in,
        "orders_completed": orders_completed,
        "orders_not_completed": orders_not_completed,
        "orders_rejected": orders_rejected,
        "orders_rejected_by_reason": _reason_counts(rejected_orders),
        "customers_rejected_by_reason": _reason_counts(rejected_customers),
    }

    return CurationResult(
        customer_revenue=customer_revenue,
        completed_orders=completed.select(
            "order_id", "customer_id", "quantity", "unit_price", "status", "revenue"
        ),
        rejected_orders=rejected_orders,
        rejected_customers=rejected_customers,
        counts=counts,
    )


def write_parquet_atomic(df: DataFrame, final_path: Path) -> None:
    """Write to a temporary directory, then rename into place. Never overwrites."""
    if final_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {final_path}")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = final_path.with_name(f"{final_path.name}.tmp-{uuid.uuid4().hex}")
    df.write.mode("errorifexists").parquet(str(tmp_path))
    tmp_path.replace(final_path)


def parquet_writer_available() -> bool:
    """Spark's Hadoop committer needs winutils.exe and HADOOP_HOME on Windows. Other platforms need nothing extra."""
    if os.name != "nt":
        return True
    hadoop_home = os.environ.get("HADOOP_HOME", "")
    return bool(hadoop_home) and (Path(hadoop_home) / "bin" / "winutils.exe").is_file()


def _write_csv_atomic(df: DataFrame, final_path: Path) -> None:
    """CSV via pandas, so no Hadoop committer is involved. Driver-side collect: fine for training-scale output."""
    if final_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {final_path}")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = final_path.with_name(f"{final_path.name}.tmp-{uuid.uuid4().hex}")
    df.toPandas().to_csv(tmp_path, index=False, encoding="utf-8")
    tmp_path.replace(final_path)


def write_output_atomic(df: DataFrame, final_base: Path) -> Path:
    """Write Parquet when Spark can, otherwise CSV. Returns the path written."""
    if parquet_writer_available():
        final_path = final_base.with_suffix(".parquet")
        write_parquet_atomic(df, final_path)
        return final_path
    LOGGER.warning("[curate_orders_revenue] winutils.exe / HADOOP_HOME not found; writing CSV instead of Parquet")
    final_path = final_base.with_suffix(".csv")
    _write_csv_atomic(df, final_path)
    return final_path


def run(
    spark: SparkSession,
    customers_path: str,
    orders_path: str,
    output_root: str,
    run_id: str,
) -> Dict[str, Any]:
    customers_raw = spark.read.option("header", True).csv(customers_path)
    orders_raw = spark.read.option("header", True).csv(orders_path)
    result = curate(customers_raw, orders_raw)

    out_dir = Path(output_root) / run_id
    if out_dir.exists():
        raise FileExistsError(f"Run {run_id} already has outputs at {out_dir}; choose a new run id")
    outputs = {
        "customer_revenue": result.customer_revenue,
        "completed_orders": result.completed_orders,
        "rejected_orders": result.rejected_orders,
        "rejected_customers": result.rejected_customers,
    }
    for name, df in outputs.items():
        written = write_output_atomic(df, out_dir / name)
        LOGGER.info("[curate_orders_revenue] wrote %s", written)
    return result.counts


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Curate completed-order revenue by customer.")
    parser.add_argument("--customers", required=True, help="Path to customers CSV (with header).")
    parser.add_argument("--orders", required=True, help="Path to orders CSV (with header).")
    parser.add_argument("--output-root", required=True, help="Root directory for outputs.")
    parser.add_argument("--run-id", required=True, help="Unique run id; reusing one fails.")
    args = parser.parse_args(argv)

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = SparkSession.builder.appName("CurateOrdersRevenue").getOrCreate()
    try:
        counts = run(spark, args.customers, args.orders, args.output_root, args.run_id)
    finally:
        spark.stop()

    for key, value in counts.items():
        LOGGER.info("%s: %s", key, value)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
