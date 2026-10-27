"""Bootstrap the Module 3 ingestion schema in the local retail PostgreSQL database.

Loads the clean sample CSVs from data/samples/ into the ingestion schema inside a single transaction.
Existing rows that conflict with the sample abort the run before any write. Nothing is dropped,
truncated, or overwritten.
"""

from __future__ import annotations

import csv
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple

import psycopg2
from psycopg2 import sql

LOGGER = logging.getLogger("bootstrap_module3")

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLES_DIR = REPO_ROOT / "data" / "samples"
EXPECTED_COUNTS = {"customers": 3, "products": 3, "orders": 5}

Row = Tuple[Any, ...]

DDL_STATEMENTS = (
    "CREATE SCHEMA IF NOT EXISTS ingestion",
    """
    CREATE TABLE IF NOT EXISTS ingestion.customers (
        customer_id INTEGER PRIMARY KEY,
        customer_name VARCHAR(100) NOT NULL,
        region VARCHAR(50),
        signup_date DATE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.products (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(100) NOT NULL,
        category VARCHAR(50)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.orders_ingested (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL REFERENCES ingestion.customers (customer_id),
        product_id INTEGER NOT NULL REFERENCES ingestion.products (product_id),
        order_date DATE NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
        status VARCHAR(20) NOT NULL
    )
    """,
)


class BootstrapError(RuntimeError):
    """Raised for sample validation failures and data conflicts with existing rows."""


@dataclass(frozen=True)
class TableSpec:
    """Source CSV file, target table, and column mapping for one load. The first target column is the key."""

    sample_key: str
    table: str
    file_name: str
    csv_columns: Tuple[str, ...]
    target_columns: Tuple[str, ...]
    parser: Callable[[Mapping[str, str]], Row]

    @property
    def table_identifier(self) -> sql.Identifier:
        return sql.Identifier("ingestion", self.table)

    @property
    def key_column(self) -> str:
        return self.target_columns[0]


def connection_params() -> Dict[str, Any]:
    """Read connection settings from PG* environment variables, using local training defaults."""
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5433")),
        "dbname": os.environ.get("PGDATABASE", "retail"),
        "user": os.environ.get("PGUSER", "training"),
        "password": os.environ.get("PGPASSWORD", "training_only"),
        "connect_timeout": 5,
    }


def _required(raw: Mapping[str, str], key: str) -> str:
    value = (raw.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is empty")
    return value


def _parse_customer(raw: Mapping[str, str]) -> Row:
    return (
        int(_required(raw, "customer_id")),
        _required(raw, "customer_name"),
        _required(raw, "region"),
        date.fromisoformat(_required(raw, "signup_date")),
    )


def _parse_product(raw: Mapping[str, str]) -> Row:
    return (
        int(_required(raw, "product_id")),
        _required(raw, "product_name"),
        _required(raw, "category"),
    )


def _parse_order(raw: Mapping[str, str]) -> Row:
    quantity = int(_required(raw, "quantity"))
    if quantity <= 0:
        raise ValueError(f"quantity must be positive, got {quantity}")
    unit_price = Decimal(_required(raw, "unit_price")).quantize(Decimal("0.01"))
    if unit_price < 0:
        raise ValueError(f"unit_price must not be negative, got {unit_price}")
    return (
        int(_required(raw, "order_id")),
        int(_required(raw, "customer_id")),
        int(_required(raw, "product_id")),
        date.fromisoformat(_required(raw, "order_date")),
        quantity,
        unit_price,
        _required(raw, "status"),
    )


CUSTOMERS, PRODUCTS, ORDERS = (
    TableSpec(
        sample_key="customers",
        table="customers",
        file_name="customers.csv",
        csv_columns=("customer_id", "customer_name", "region", "signup_date"),
        target_columns=("customer_id", "customer_name", "region", "signup_date"),
        parser=_parse_customer,
    ),
    TableSpec(
        sample_key="products",
        table="products",
        file_name="products.csv",
        csv_columns=("product_id", "product_name", "category"),
        target_columns=("product_id", "product_name", "category"),
        parser=_parse_product,
    ),
    TableSpec(
        sample_key="orders",
        table="orders_ingested",
        file_name="orders.csv",
        csv_columns=("order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "status"),
        target_columns=("order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "status"),
        parser=_parse_order,
    ),
)
TABLE_SPECS: Tuple[TableSpec, ...] = (CUSTOMERS, PRODUCTS, ORDERS)


def read_sample(spec: TableSpec, samples_dir: Path) -> List[Row]:
    """Parse one sample CSV into typed rows, rejecting the whole file on the first bad record."""
    path = samples_dir / spec.file_name
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != spec.csv_columns:
            raise BootstrapError(
                f"[bootstrap_module3] {spec.file_name} header {reader.fieldnames} does not match {spec.csv_columns}"
            )
        rows: List[Row] = []
        for line_no, raw in enumerate(reader, start=2):
            try:
                rows.append(spec.parser(raw))
            except (ValueError, InvalidOperation) as exc:
                raise BootstrapError(f"[bootstrap_module3] {spec.file_name} line {line_no}: {exc}") from exc
    return rows


def validate_sample(data: Mapping[str, List[Row]]) -> None:
    """Check expected row counts, primary-key uniqueness, and foreign-key references in the samples."""
    for spec in TABLE_SPECS:
        actual = len(data[spec.sample_key])
        expected = EXPECTED_COUNTS[spec.sample_key]
        if actual != expected:
            raise BootstrapError(f"[bootstrap_module3] {spec.sample_key}: expected {expected} rows, found {actual}")
        keys = [row[0] for row in data[spec.sample_key]]
        if len(keys) != len(set(keys)):
            raise BootstrapError(f"[bootstrap_module3] {spec.file_name}: duplicate {spec.key_column} values")

    customer_ids = {row[0] for row in data[CUSTOMERS.sample_key]}
    product_ids = {row[0] for row in data[PRODUCTS.sample_key]}
    for row in data[ORDERS.sample_key]:
        if row[1] not in customer_ids:
            raise BootstrapError(f"[bootstrap_module3] order {row[0]} references unknown customer_id {row[1]}")
        if row[2] not in product_ids:
            raise BootstrapError(f"[bootstrap_module3] order {row[0]} references unknown product_id {row[2]}")


def load_samples(samples_dir: Path = SAMPLES_DIR) -> Dict[str, List[Row]]:
    """Read and validate all three clean sample files, keyed by TableSpec.sample_key."""
    data = {spec.sample_key: read_sample(spec, samples_dir) for spec in TABLE_SPECS}
    validate_sample(data)
    return data


def find_conflicts(cur: Any, spec: TableSpec, sample_rows: Sequence[Row]) -> List[str]:
    """Return descriptions of existing rows whose key matches a sample row but whose values differ."""
    keys = [row[0] for row in sample_rows]
    select_sql = sql.SQL("SELECT {cols} FROM {table} WHERE {key} = ANY(%s)").format(
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in spec.target_columns),
        table=spec.table_identifier,
        key=sql.Identifier(spec.key_column),
    )
    cur.execute(select_sql, (keys,))
    existing: Dict[Any, Row] = {row[0]: tuple(row) for row in cur.fetchall()}
    conflicts: List[str] = []
    for sample_row in sample_rows:
        current = existing.get(sample_row[0])
        if current is not None and current != sample_row:
            conflicts.append(
                f"ingestion.{spec.table} {spec.key_column}={sample_row[0]}: existing={current} sample={sample_row}"
            )
    return conflicts


def build_insert(spec: TableSpec) -> sql.Composed:
    """Build a parameterized insert that skips rows whose key already exists with identical values."""
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in spec.target_columns)
    return sql.SQL("INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT ({key}) DO NOTHING").format(
        table=spec.table_identifier,
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in spec.target_columns),
        vals=placeholders,
        key=sql.Identifier(spec.key_column),
    )


def run_bootstrap(samples_dir: Path = SAMPLES_DIR) -> Dict[str, int]:
    """Create the ingestion schema and tables, then load samples in one transaction. Returns row counts."""
    data = load_samples(samples_dir)
    LOGGER.info("[bootstrap_module3] samples validated: customers=%d products=%d orders=%d",
                len(data["customers"]), len(data["products"]), len(data["orders"]))

    conn = psycopg2.connect(**connection_params())
    try:
        with conn:
            counts: Dict[str, int] = {}
            with conn.cursor() as cur:
                for statement in DDL_STATEMENTS:
                    cur.execute(statement)

                all_conflicts: List[str] = []
                for spec in TABLE_SPECS:
                    all_conflicts.extend(find_conflicts(cur, spec, data[spec.sample_key]))
                if all_conflicts:
                    raise BootstrapError(
                        "[bootstrap_module3] existing rows conflict with the sample; no changes made:\n  "
                        + "\n  ".join(all_conflicts)
                    )

                for spec in TABLE_SPECS:
                    cur.executemany(build_insert(spec), data[spec.sample_key])

                for spec in TABLE_SPECS:
                    cur.execute(sql.SQL("SELECT COUNT(*) FROM {table}").format(table=spec.table_identifier))
                    counts[f"ingestion.{spec.table}"] = int(cur.fetchone()[0])
        return counts
    finally:
        conn.close()


def main() -> int:
    """Run the bootstrap and return a process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    try:
        counts = run_bootstrap()
    except BootstrapError as exc:
        LOGGER.error("%s", exc)
        return 1
    except psycopg2.Error as exc:
        LOGGER.error("[bootstrap_module3] database error, transaction rolled back: %s", exc)
        return 1
    except OSError as exc:
        LOGGER.error("[bootstrap_module3] could not read sample files: %s", exc)
        return 1

    for table, count in counts.items():
        LOGGER.info("[bootstrap_module3] %s rows: %d", table, count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
