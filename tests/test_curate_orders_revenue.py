"""Spark tests for scripts/curate_orders_revenue.py."""

import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from pyspark.sql import SparkSession  # noqa: E402

import curate_orders_revenue as cor  # noqa: E402

CUSTOMER_SCHEMA = "customer_id string, customer_name string, region string"
ORDER_SCHEMA = "order_id string, customer_id string, quantity string, unit_price string, status string"


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("TestCurateOrdersRevenue")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def customers(spark, rows):
    return spark.createDataFrame(rows, CUSTOMER_SCHEMA)


def orders(spark, rows):
    return spark.createDataFrame(rows, ORDER_SCHEMA)


BASE_CUSTOMERS = [("501", "Asha", "South"), ("502", "Rahul", "North"), ("503", "Meera", "West")]


def test_happy_path_revenue_status_and_customer_retention(spark):
    o = orders(
        spark,
        [
            ("1001", "501", "2", "125.00", "COMPLETED"),
            ("1002", "502", "1", "175.50", "COMPLETED"),
            ("1003", "501", "1", "99.00", "COMPLETED"),
            ("1004", "503", "2", "80.00", "CANCELLED"),
            ("1005", "501", "1", "50.00", "PENDING"),
        ],
    )
    result = cor.curate(customers(spark, BASE_CUSTOMERS), o)

    completed = {r["order_id"]: r["revenue"] for r in result.completed_orders.collect()}
    assert completed == {1001: Decimal("250.00"), 1002: Decimal("175.50"), 1003: Decimal("99.00")}

    summary = {r["customer_id"]: r for r in result.customer_revenue.collect()}
    assert set(summary) == {501, 502, 503}  # every customer retained
    assert summary[501]["total_revenue"] == Decimal("349.00")
    assert summary[501]["completed_order_count"] == 2
    assert summary[503]["total_revenue"] == Decimal("0.00")  # only a cancelled order
    assert summary[503]["completed_order_count"] == 0

    assert result.counts["orders_completed"] == 3
    assert result.counts["orders_not_completed"] == 2
    assert result.counts["orders_rejected"] == 0


def test_decimal_precision_is_exact(spark):
    o = orders(spark, [("1", "501", "3", "0.10", "COMPLETED")])
    result = cor.curate(customers(spark, BASE_CUSTOMERS), o)
    assert result.completed_orders.first()["revenue"] == Decimal("0.30")


def test_missing_required_column_fails_fast(spark):
    bad_customers = spark.createDataFrame([("501", "Asha")], "customer_id string, customer_name string")
    with pytest.raises(ValueError, match="customers is missing required columns"):
        cor.curate(bad_customers, orders(spark, []))

    bad_orders = spark.createDataFrame([("1",)], "order_id string")
    with pytest.raises(ValueError, match="orders is missing required columns"):
        cor.curate(customers(spark, BASE_CUSTOMERS), bad_orders)


@pytest.mark.parametrize(
    "row, expected_reason",
    [
        (("", "501", "1", "10.00", "COMPLETED"), "missing_order_id"),
        (("   ", "501", "1", "10.00", "COMPLETED"), "missing_order_id"),
        (("2001", "", "1", "10.00", "COMPLETED"), "missing_customer_id"),
        (("2002", "501", "", "10.00", "COMPLETED"), "missing_quantity"),
        (("2003", "501", "1", "", "COMPLETED"), "missing_unit_price"),
        (("2004", "501", "1", "10.00", ""), "missing_status"),
        (("abc", "501", "1", "10.00", "COMPLETED"), "invalid_order_id"),
        (("2006", "5O1", "1", "10.00", "COMPLETED"), "invalid_customer_id"),
        (("2007", "501", "2.5", "10.00", "COMPLETED"), "invalid_quantity"),
        (("2008", "501", "1", "NaN", "COMPLETED"), "invalid_unit_price"),
        (("2009", "501", "1", "Infinity", "COMPLETED"), "invalid_unit_price"),
        (("2010", "501", "1", "10.001", "COMPLETED"), "invalid_unit_price"),
        (("2011", "501", "0", "10.00", "COMPLETED"), "quantity_not_positive"),
        (("2012", "501", "-3", "10.00", "COMPLETED"), "quantity_not_positive"),
        (("2013", "501", "1", "-10.00", "COMPLETED"), "unit_price_negative"),
        (("2014", "501", "1", "10.00", "SHIPPED"), "invalid_status"),
        (("2015", "999", "1", "10.00", "COMPLETED"), "unknown_customer_id"),
    ],
)
def test_bad_rows_are_rejected_with_reason(spark, row, expected_reason):
    good = ("1000", "501", "1", "10.00", "COMPLETED")
    result = cor.curate(customers(spark, BASE_CUSTOMERS), orders(spark, [good, row]))

    rejected = result.rejected_orders.select("reasons").collect()
    assert len(rejected) == 1
    assert expected_reason in rejected[0]["reasons"]
    assert result.counts["orders_rejected"] == 1
    assert result.counts["orders_completed"] == 1
    assert result.counts["orders_rejected_by_reason"][expected_reason] == 1


def test_duplicate_order_id_quarantines_whole_group(spark):
    o = orders(
        spark,
        [
            ("3001", "501", "1", "10.00", "COMPLETED"),
            ("3001", "501", "1", "10.00", "COMPLETED"),  # exact duplicate
            ("3002", "502", "2", "5.00", "COMPLETED"),
            ("3002", "502", "4", "5.00", "COMPLETED"),  # conflicting duplicate
            ("3003", "503", "1", "7.00", "COMPLETED"),
        ],
    )
    result = cor.curate(customers(spark, BASE_CUSTOMERS), o)

    assert result.counts["orders_rejected"] == 4
    assert result.counts["orders_rejected_by_reason"]["duplicate_order_id"] == 4
    completed_ids = {r["order_id"] for r in result.completed_orders.collect()}
    assert completed_ids == {3003}  # no survivor chosen for duplicated keys
    summary = {r["customer_id"]: r["total_revenue"] for r in result.customer_revenue.collect()}
    assert summary == {501: Decimal("0.00"), 502: Decimal("0.00"), 503: Decimal("7.00")}


def test_duplicate_customer_id_rejects_customer_rows(spark):
    dup_customers = customers(spark, BASE_CUSTOMERS + [("501", "Asha Dup", "East")])
    o = orders(spark, [("1", "501", "1", "10.00", "COMPLETED")])
    result = cor.curate(dup_customers, o)

    assert result.counts["customers_rejected_by_reason"] == {"duplicate_customer_id": 2}
    assert result.counts["customers_in"] == 4
    assert {r["customer_id"] for r in result.customer_revenue.collect()} == {502, 503}
    # Order for customer 501 has no valid customer, so it is quarantined, not dropped.
    assert result.counts["orders_rejected_by_reason"] == {"unknown_customer_id": 1}


def test_empty_orders_keep_all_customers_at_zero(spark):
    result = cor.curate(customers(spark, BASE_CUSTOMERS), orders(spark, []))
    rows = result.customer_revenue.collect()
    assert len(rows) == 3
    assert all(r["total_revenue"] == Decimal("0.00") for r in rows)
    assert result.counts["orders_in"] == 0


def test_whitespace_is_trimmed_and_status_is_case_normalised(spark):
    o = orders(spark, [(" 4001 ", " 501 ", " 2 ", " 3.50 ", " completed ")])
    result = cor.curate(customers(spark, [(" 501 ", " Asha ", "South")]), o)
    row = result.completed_orders.first()
    assert row["order_id"] == 4001
    assert row["revenue"] == Decimal("7.00")


def test_reconciliation_holds_for_mixed_input(spark):
    o = orders(
        spark,
        [
            ("5001", "501", "1", "1.00", "COMPLETED"),
            ("5002", "501", "1", "1.00", "CANCELLED"),
            ("5003", "", "1", "1.00", "COMPLETED"),
            ("5004", "502", "x", "1.00", "COMPLETED"),
        ],
    )
    c = cor.curate(customers(spark, BASE_CUSTOMERS), o).counts
    assert c["orders_in"] == c["orders_completed"] + c["orders_not_completed"] + c["orders_rejected"]
    assert c["orders_in"] == 4
    assert c["orders_completed"] == 1
    assert c["orders_not_completed"] == 1
    assert c["orders_rejected"] == 2


@pytest.mark.skipif(
    not cor.parquet_writer_available(), reason="Parquet needs winutils.exe and HADOOP_HOME on Windows"
)
def test_atomic_parquet_write_refuses_overwrite(spark, tmp_path):
    df = spark.createDataFrame([(1,)], "id int")
    target = tmp_path / "run1" / "out.parquet"

    cor.write_parquet_atomic(df, target)
    assert spark.read.parquet(str(target)).count() == 1

    with pytest.raises(FileExistsError):
        cor.write_parquet_atomic(df, target)
    leftovers = [p for p in (tmp_path / "run1").iterdir() if ".tmp-" in p.name]
    assert leftovers == []


def test_atomic_csv_write_refuses_overwrite(spark, tmp_path):
    df = spark.createDataFrame([(1, "a")], "id int, name string")
    target = tmp_path / "out.csv"

    cor._write_csv_atomic(df, target)
    assert target.read_text(encoding="utf-8").splitlines() == ["id,name", "1,a"]

    with pytest.raises(FileExistsError):
        cor._write_csv_atomic(df, target)
    assert [p for p in tmp_path.iterdir() if ".tmp-" in p.name] == []


def test_parquet_detection_requires_winutils_on_windows(monkeypatch, tmp_path):
    if os.name != "nt":
        pytest.skip("winutils check only applies on Windows")
    monkeypatch.delenv("HADOOP_HOME", raising=False)
    assert cor.parquet_writer_available() is False

    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "winutils.exe").write_bytes(b"")
    monkeypatch.setenv("HADOOP_HOME", str(tmp_path))
    assert cor.parquet_writer_available() is True


def _write_run_inputs(tmp_path):
    cust_path = tmp_path / "customers.csv"
    ord_path = tmp_path / "orders.csv"
    cust_path.write_text("customer_id,customer_name,region\n501,Asha,South\n502,Rahul,North\n", encoding="utf-8")
    ord_path.write_text(
        "order_id,customer_id,quantity,unit_price,status\n"
        "1,501,2,125.00,COMPLETED\n"
        "2,502,x,10.00,COMPLETED\n",
        encoding="utf-8",
    )
    return cust_path, ord_path


def test_run_falls_back_to_csv_without_winutils(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(cor, "parquet_writer_available", lambda: False)
    cust_path, ord_path = _write_run_inputs(tmp_path)
    out_root = tmp_path / "out"

    counts = cor.run(spark, str(cust_path), str(ord_path), str(out_root), "run-001")

    run_dir = out_root / "run-001"
    assert counts["orders_completed"] == 1
    assert counts["orders_rejected_by_reason"] == {"invalid_quantity": 1}
    assert spark.read.option("header", True).csv(str(run_dir / "customer_revenue.csv")).count() == 2
    assert spark.read.option("header", True).csv(str(run_dir / "rejected_orders.csv")).count() == 1
    assert not list(run_dir.glob("*.parquet"))

    with pytest.raises(FileExistsError):
        cor.run(spark, str(cust_path), str(ord_path), str(out_root), "run-001")


@pytest.mark.skipif(
    not cor.parquet_writer_available(), reason="Parquet needs winutils.exe and HADOOP_HOME on Windows"
)
def test_run_writes_parquet_when_available(spark, tmp_path):
    cust_path, ord_path = _write_run_inputs(tmp_path)
    out_root = tmp_path / "out"

    cor.run(spark, str(cust_path), str(ord_path), str(out_root), "run-002")

    run_dir = out_root / "run-002"
    assert spark.read.parquet(str(run_dir / "customer_revenue.parquet")).count() == 2
    assert spark.read.parquet(str(run_dir / "rejected_orders.parquet")).count() == 1
