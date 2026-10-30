"""Tests for the retail customer analytics DAG (ingest -> transform -> validate).

The DAG module imports Airflow, so these tests run where Airflow is installed
(the Docker Airflow image). Paths are redirected to a temporary directory so the
tests never write into the real ``output/`` folder.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "dags"))

import retail_customer_analytics_dag as dag_module  # noqa: E402
from airflow.exceptions import AirflowFailException  # noqa: E402

SOURCE_OK = REPO_ROOT / "data" / "raw" / "orders.csv"
SOURCE_BAD = REPO_ROOT / "data" / "samples" / "orders_quality.csv"


class RevenueRuleTests(unittest.TestCase):
    def test_completed_order_is_quantity_times_unit_price(self) -> None:
        row = {"quantity": "2", "unit_price": "125.00", "status": "COMPLETED"}
        self.assertEqual(dag_module._compute_revenue(row), "250.00")

    def test_non_completed_orders_have_zero_revenue(self) -> None:
        for status in ("CANCELLED", "PENDING"):
            row = {"quantity": "2", "unit_price": "80.00", "status": status}
            self.assertEqual(dag_module._compute_revenue(row), "0.00", status)

    def test_revenue_rounds_half_up_to_cents(self) -> None:
        row = {"quantity": "3", "unit_price": "0.335", "status": "COMPLETED"}
        self.assertEqual(dag_module._compute_revenue(row), "1.01")

    def test_unpriceable_completed_order_returns_empty(self) -> None:
        bad_inputs = [
            {"quantity": "x", "unit_price": "10", "status": "COMPLETED"},
            {"quantity": "1", "unit_price": "", "status": "COMPLETED"},
            {"quantity": "1", "unit_price": "NaN", "status": "COMPLETED"},
        ]
        for row in bad_inputs:
            self.assertEqual(dag_module._compute_revenue(row), "", row)


class FieldParsingTests(unittest.TestCase):
    def test_positive_int(self) -> None:
        self.assertTrue(dag_module._is_positive_int("2"))
        self.assertFalse(dag_module._is_positive_int("0"))
        self.assertFalse(dag_module._is_positive_int("-1"))
        self.assertFalse(dag_module._is_positive_int("2.5"))
        self.assertFalse(dag_module._is_positive_int("abc"))

    def test_non_negative_decimal(self) -> None:
        self.assertTrue(dag_module._is_non_negative_decimal("0"))
        self.assertTrue(dag_module._is_non_negative_decimal("125.00"))
        self.assertFalse(dag_module._is_non_negative_decimal("-10.00"))
        self.assertFalse(dag_module._is_non_negative_decimal("NaN"))
        self.assertFalse(dag_module._is_non_negative_decimal("abc"))


class PipelineRunTests(unittest.TestCase):
    """Runs the three task callables against temporary paths."""

    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="retail_dag_test_")
        self.base_dir = Path(self._tmp) / "out"
        self._saved_env = {k: os.environ.get(k) for k in (dag_module.SOURCE_PATH_ENV, dag_module.BASE_DIR_ENV)}

    def tearDown(self) -> None:
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _configure(self, source: Path) -> None:
        os.environ[dag_module.SOURCE_PATH_ENV] = str(source)
        os.environ[dag_module.BASE_DIR_ENV] = str(self.base_dir)

    def _run_stages(self, ds: str, source: Path) -> None:
        self._configure(source)
        dag_module.ingest_orders(ds)
        dag_module.transform_orders(ds)
        dag_module.validate_orders(ds)

    def _transformed_rows(self, ds: str) -> list:
        paths = dag_module.partition_paths(dag_module.load_settings(), ds)
        _, rows = dag_module._read_csv(paths.transformed_file)
        return rows

    def _report(self, ds: str) -> dict:
        paths = dag_module.partition_paths(dag_module.load_settings(), ds)
        return json.loads(paths.report_file.read_text(encoding="utf-8"))

    def test_happy_path_writes_revenue_and_passes_checks(self) -> None:
        self._run_stages("2025-05-01", SOURCE_OK)

        rows = {row["order_id"]: row for row in self._transformed_rows("2025-05-01")}
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows["1001"]["revenue"], "250.00")
        self.assertEqual(rows["1004"]["revenue"], "0.00")
        self.assertEqual(self._report("2025-05-01")["status"], "PASSED")

    def test_rerun_same_date_does_not_duplicate_rows(self) -> None:
        self._run_stages("2025-05-01", SOURCE_OK)
        self._run_stages("2025-05-01", SOURCE_OK)

        self.assertEqual(len(self._transformed_rows("2025-05-01")), 5)

    def test_validation_fails_task_on_bad_data_and_reports_each_check(self) -> None:
        self._configure(SOURCE_BAD)
        dag_module.ingest_orders("2025-05-02")
        dag_module.transform_orders("2025-05-02")

        with self.assertRaises(AirflowFailException):
            dag_module.validate_orders("2025-05-02")

        report = self._report("2025-05-02")
        self.assertEqual(report["status"], "FAILED")
        failed = {name for name, check in report["checks"].items() if not check["passed"]}
        self.assertEqual(
            failed,
            {
                dag_module.CHECK_REQUIRED,
                dag_module.CHECK_UNIQUE,
                dag_module.CHECK_QUANTITY,
                dag_module.CHECK_PRICE,
                dag_module.CHECK_STATUS,
            },
        )
        self.assertIn("line 8: 'customer_id' is empty", report["checks"][dag_module.CHECK_REQUIRED]["examples"][0])

    def test_transform_fails_fast_when_required_column_missing(self) -> None:
        self._configure(SOURCE_OK)
        paths = dag_module.partition_paths(dag_module.load_settings(), "2025-05-03")
        paths.raw_file.parent.mkdir(parents=True, exist_ok=True)
        paths.raw_file.write_text("order_id,quantity\n1001,2\n", encoding="utf-8")

        with self.assertRaises(AirflowFailException):
            dag_module.transform_orders("2025-05-03")

    def test_ingest_fails_when_source_missing(self) -> None:
        self._configure(Path(self._tmp) / "does_not_exist.csv")
        with self.assertRaises(FileNotFoundError):
            dag_module.ingest_orders("2025-05-04")

    def test_transform_fails_when_raw_partition_missing(self) -> None:
        self._configure(SOURCE_OK)
        with self.assertRaises(FileNotFoundError):
            dag_module.transform_orders("2025-05-05")


class DagDefinitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dag = dag_module.dag

    def test_schedule_catchup_and_concurrency(self) -> None:
        self.assertEqual(self.dag.timetable.summary, "0 6 * * *")
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_retry_policy(self) -> None:
        self.assertEqual(self.dag.default_args["retries"], 2)
        self.assertEqual(self.dag.default_args["retry_delay"], timedelta(minutes=5))
        for task in self.dag.tasks:
            self.assertEqual(task.retries, 2, task.task_id)
            self.assertEqual(task.retry_delay, timedelta(minutes=5), task.task_id)

    def test_every_task_has_execution_timeout(self) -> None:
        for task in self.dag.tasks:
            self.assertIsNotNone(task.execution_timeout, task.task_id)
            self.assertGreater(task.execution_timeout, timedelta(0), task.task_id)

    def test_dependency_order(self) -> None:
        ingest = self.dag.get_task("ingest_orders")
        transform = self.dag.get_task("transform_orders")
        validate = self.dag.get_task("validate_orders")
        self.assertEqual(transform.upstream_task_ids, {"ingest_orders"})
        self.assertEqual(validate.upstream_task_ids, {"transform_orders"})
        self.assertEqual(ingest.upstream_task_ids, set())


if __name__ == "__main__":
    unittest.main()
