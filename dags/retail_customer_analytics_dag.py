"""Retail customer analytics pipeline (daily at 06:00 UTC).

Stages, run strictly in order for each logical date (``ds``):

1. ``ingest_orders``    - copy the source order extract into the raw zone.
2. ``transform_orders`` - add ``revenue`` = quantity * unit_price for COMPLETED orders
   (0.00 for other statuses) and write the transformed partition.
3. ``validate_orders``  - run mandatory quality checks, write a JSON report, and fail
   the task if any check fails.

Configuration (optional environment variables, read at task run time):

- ``RETAIL_SOURCE_ORDERS_PATH``: source order CSV.
  Default: ``/opt/airflow/project/data/raw/orders.csv``.
- ``RETAIL_PIPELINE_BASE_DIR``: root for pipeline outputs.
  Default: ``/opt/airflow/project/output/retail_customer_analytics``.

Container paths: the repo root is mounted at ``/opt/airflow/project`` (read-write) by
docker-compose.yml, so outputs appear under ``./output/`` on the host.

Idempotency: every stage writes a partition keyed by ``ds=YYYY-MM-DD``. Files are written
to ``<name>.tmp`` and renamed into place, so a retry replaces that date's partition and never
appends to it. Other dates are untouched. ``max_active_runs=1`` prevents concurrent writes.

Dependencies: tasks do not exchange XCom. Each task derives the same partition paths from
``ds``; the ordering ``ingest >> transform >> validate`` is the only coupling.

No credentials are used. The pipeline reads and writes local files only.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Sequence, Set, Tuple

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

DAG_ID = "retail_customer_analytics"
LOGGER = logging.getLogger(DAG_ID)

PROJECT_ROOT = Path("/opt/airflow/project")
DEFAULT_SOURCE_PATH = PROJECT_ROOT / "data" / "raw" / "orders.csv"
DEFAULT_BASE_DIR = PROJECT_ROOT / "output" / "retail_customer_analytics"
SOURCE_PATH_ENV = "RETAIL_SOURCE_ORDERS_PATH"
BASE_DIR_ENV = "RETAIL_PIPELINE_BASE_DIR"

REQUIRED_COLUMNS: Tuple[str, ...] = (
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "status",
)
REVENUE_COLUMN = "revenue"
REVENUE_STATUS = "COMPLETED"
VALID_STATUSES: Set[str] = {"COMPLETED", "CANCELLED", "PENDING"}
CENT = Decimal("0.01")
MAX_REPORTED_ERRORS = 20

CHECK_OUTPUT_NOT_EMPTY = "output_not_empty"
CHECK_REQUIRED = "required_fields_present"
CHECK_UNIQUE = "order_id_unique"
CHECK_QUANTITY = "quantity_positive"
CHECK_PRICE = "unit_price_non_negative"
CHECK_STATUS = "status_valid"
ALL_CHECKS: Tuple[str, ...] = (
    CHECK_OUTPUT_NOT_EMPTY,
    CHECK_REQUIRED,
    CHECK_UNIQUE,
    CHECK_QUANTITY,
    CHECK_PRICE,
    CHECK_STATUS,
)


@dataclass(frozen=True)
class PipelineSettings:
    """Resolved input and output locations."""

    source_path: Path
    base_dir: Path


@dataclass(frozen=True)
class RunPaths:
    """Partition file locations for one logical date."""

    raw_file: Path
    transformed_file: Path
    report_file: Path


def load_settings() -> PipelineSettings:
    """Resolve paths from environment variables, falling back to project defaults."""
    return PipelineSettings(
        source_path=Path(os.environ.get(SOURCE_PATH_ENV) or DEFAULT_SOURCE_PATH),
        base_dir=Path(os.environ.get(BASE_DIR_ENV) or DEFAULT_BASE_DIR),
    )


def partition_paths(settings: PipelineSettings, ds: str) -> RunPaths:
    """Return the deterministic partition paths for logical date ``ds``."""
    partition = f"ds={ds}"
    return RunPaths(
        raw_file=settings.base_dir / "raw" / partition / "orders_raw.csv",
        transformed_file=settings.base_dir / "transformed" / partition / "orders_revenue.csv",
        report_file=settings.base_dir / "quality" / partition / "validation_report.json",
    )


def _tmp_path(target: Path) -> Path:
    return target.with_name(target.name + ".tmp")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return fieldnames, rows


def _write_csv_atomic(target: Path, fieldnames: Sequence[str], rows: List[Dict[str, Any]]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(target)
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(target)


def _write_json_atomic(target: Path, payload: Dict[str, Any]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(target)
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    tmp.replace(target)


def _missing_columns(fieldnames: Sequence[str], required: Sequence[str]) -> List[str]:
    return [column for column in required if column not in fieldnames]


def _compute_revenue(row: Dict[str, Any]) -> str:
    """Return revenue as a 2-decimal string.

    Non-COMPLETED orders return ``0.00``. Returns ``""`` when a COMPLETED order cannot be
    priced; validation then rejects that row.
    """
    if (row.get("status") or "").strip() != REVENUE_STATUS:
        return "0.00"
    try:
        quantity = int((row.get("quantity") or "").strip())
        unit_price = Decimal((row.get("unit_price") or "").strip())
        if not unit_price.is_finite():
            return ""
        revenue = (Decimal(quantity) * unit_price).quantize(CENT, rounding=ROUND_HALF_UP)
    except (ValueError, InvalidOperation):
        return ""
    return str(revenue)


def _is_positive_int(text: str) -> bool:
    try:
        return int(text) > 0
    except ValueError:
        return False


def _is_non_negative_decimal(text: str) -> bool:
    try:
        value = Decimal(text)
    except InvalidOperation:
        return False
    return value.is_finite() and value >= 0


def ingest_orders(ds: str, **_: Any) -> None:
    """Copy the source order extract into the raw partition for logical date ``ds``."""
    settings = load_settings()
    paths = partition_paths(settings, ds)
    LOGGER.info("[%s] ingest start ds=%s source=%s", DAG_ID, ds, settings.source_path)

    if not settings.source_path.is_file():
        raise FileNotFoundError(f"Source order file not found: {settings.source_path}")
    if paths.raw_file.exists():
        LOGGER.info("[%s] raw partition already exists, replacing it: %s", DAG_ID, paths.raw_file)

    paths.raw_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(paths.raw_file)
    shutil.copyfile(settings.source_path, tmp)
    tmp.replace(paths.raw_file)

    _, rows = _read_csv(paths.raw_file)
    LOGGER.info(
        "[%s] ingest complete ds=%s rows=%d sha256=%s output=%s",
        DAG_ID, ds, len(rows), _sha256(paths.raw_file), paths.raw_file,
    )


def transform_orders(ds: str, **_: Any) -> None:
    """Add the ``revenue`` column to the raw partition and write the transformed partition."""
    settings = load_settings()
    paths = partition_paths(settings, ds)
    LOGGER.info("[%s] transform start ds=%s input=%s", DAG_ID, ds, paths.raw_file)

    if not paths.raw_file.is_file():
        raise FileNotFoundError(f"Raw partition missing, run ingestion first: {paths.raw_file}")

    fieldnames, rows = _read_csv(paths.raw_file)
    missing = _missing_columns(fieldnames, REQUIRED_COLUMNS)
    if missing:
        # Non-retryable: the source schema is wrong, so a retry will fail the same way.
        raise AirflowFailException(f"Raw file missing required columns: {', '.join(missing)}")

    output_fields = list(fieldnames)
    if REVENUE_COLUMN not in output_fields:
        output_fields.append(REVENUE_COLUMN)

    transformed: List[Dict[str, Any]] = []
    unpriced = 0
    completed = 0
    for row in rows:
        revenue = _compute_revenue(row)
        if revenue == "":
            unpriced += 1
        if (row.get("status") or "").strip() == REVENUE_STATUS:
            completed += 1
        out_row: Dict[str, Any] = dict(row)
        out_row[REVENUE_COLUMN] = revenue
        transformed.append(out_row)

    if unpriced:
        LOGGER.warning(
            "[%s] %d row(s) could not be priced (bad quantity or unit_price); validation will reject them",
            DAG_ID, unpriced,
        )

    _write_csv_atomic(paths.transformed_file, output_fields, transformed)
    LOGGER.info(
        "[%s] transform complete ds=%s rows=%d completed=%d output=%s",
        DAG_ID, ds, len(transformed), completed, paths.transformed_file,
    )


def _check_rows(rows: List[Dict[str, str]], failures: Dict[str, List[str]]) -> None:
    seen_order_ids: Dict[str, int] = {}
    for line_no, row in enumerate(rows, start=2):  # line 1 is the header
        for column in REQUIRED_COLUMNS:
            if not (row.get(column) or "").strip():
                failures[CHECK_REQUIRED].append(f"line {line_no}: '{column}' is empty")

        order_id = (row.get("order_id") or "").strip()
        if order_id:
            if order_id in seen_order_ids:
                failures[CHECK_UNIQUE].append(
                    f"line {line_no}: order_id {order_id} already seen on line {seen_order_ids[order_id]}"
                )
            else:
                seen_order_ids[order_id] = line_no

        quantity = (row.get("quantity") or "").strip()
        if quantity and not _is_positive_int(quantity):
            failures[CHECK_QUANTITY].append(f"line {line_no}: quantity '{quantity}' is not a positive integer")

        unit_price = (row.get("unit_price") or "").strip()
        if unit_price and not _is_non_negative_decimal(unit_price):
            failures[CHECK_PRICE].append(f"line {line_no}: unit_price '{unit_price}' is not a non-negative number")

        status = (row.get("status") or "").strip()
        if status and status not in VALID_STATUSES:
            failures[CHECK_STATUS].append(f"line {line_no}: status '{status}' is not one of {sorted(VALID_STATUSES)}")


def validate_orders(ds: str, **_: Any) -> None:
    """Run mandatory quality checks on the transformed partition and fail the task on any failure."""
    settings = load_settings()
    paths = partition_paths(settings, ds)
    LOGGER.info("[%s] validation start ds=%s input=%s", DAG_ID, ds, paths.transformed_file)

    if not paths.transformed_file.is_file():
        raise FileNotFoundError(f"Transformed partition missing, run transformation first: {paths.transformed_file}")

    fieldnames, rows = _read_csv(paths.transformed_file)
    failures: Dict[str, List[str]] = {name: [] for name in ALL_CHECKS}

    if not rows:
        failures[CHECK_OUTPUT_NOT_EMPTY].append("transformed file contains no order rows")

    missing = _missing_columns(fieldnames, (*REQUIRED_COLUMNS, REVENUE_COLUMN))
    if missing:
        failures[CHECK_REQUIRED].append(f"missing columns: {', '.join(missing)}")
    else:
        _check_rows(rows, failures)

    failed_checks = [name for name in ALL_CHECKS if failures[name]]
    _write_json_atomic(
        paths.report_file,
        {
            "dag_id": DAG_ID,
            "ds": ds,
            "input_file": str(paths.transformed_file),
            "row_count": len(rows),
            "status": "FAILED" if failed_checks else "PASSED",
            "checks": {
                name: {
                    "passed": not msgs,
                    "issue_count": len(msgs),
                    "examples": msgs[:MAX_REPORTED_ERRORS],
                }
                for name, msgs in failures.items()
            },
            "validated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    LOGGER.info("[%s] validation report written: %s", DAG_ID, paths.report_file)

    if failed_checks:
        for name in failed_checks:
            LOGGER.error(
                "[%s] check %s failed with %d issue(s); first: %s",
                DAG_ID, name, len(failures[name]), failures[name][0],
            )
        # Non-retryable: the data is bad, so retries would not help.
        raise AirflowFailException(
            f"Mandatory quality checks failed for ds={ds}: {', '.join(failed_checks)}. "
            f"See report: {paths.report_file}"
        )

    LOGGER.info("[%s] all %d quality checks passed for ds=%s rows=%d", DAG_ID, len(ALL_CHECKS), ds, len(rows))


DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    description="Retail orders: ingest -> revenue transform -> quality validation (daily 06:00 UTC)",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["training", "retail", "data-quality"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_orders",
        python_callable=ingest_orders,
        execution_timeout=timedelta(minutes=30),
    )
    transform = PythonOperator(
        task_id="transform_orders",
        python_callable=transform_orders,
        execution_timeout=timedelta(minutes=30),
    )
    validate = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders,
        execution_timeout=timedelta(minutes=15),
    )

    ingest >> transform >> validate
