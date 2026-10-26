"""Tests for the configuration-driven ingestion framework."""

import csv
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ingestion.config import ConfigError, validate_config
from ingestion.pipeline import run_ingestion
from ingestion.errors import IngestError

HEADER = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "status"]
COLUMNS = {
    "order_id": "int",
    "customer_id": "int",
    "product_id": "int",
    "order_date": "date",
    "quantity": "int",
    "unit_price": "amount",
    "status": "text",
}
GOOD_ROWS = [
    ["1001", "501", "2001", "2024-01-05", "2", "125.00", "COMPLETED"],
    ["1002", "502", "2002", "2024-01-06", "1", "80.00", "CANCELLED"],
    ["1003", "503", "2003", "2024-01-07", "4", "10.50", "COMPLETED"],
]


def csv_config(src: Path, out: Path, **load):
    return {
        "name": "orders",
        "source": {"type": "csv", "path": str(src)},
        "columns": dict(COLUMNS),
        "key_column": "order_id",
        "unique_key": False,
        "load": load or {"mode": "full"},
        "output": {"directory": str(out)},
    }


def write_csv(path: Path, rows, header=HEADER):
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def pg_config(**overrides):
    cfg = {
        "name": "orders_pg",
        "source": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "sales",
            "user": "ingest_reader",
            "password_env": "TEST_ORDERS_PG_PASSWORD",
            "schema": "public",
            "table": "orders",
        },
        "columns": dict(COLUMNS),
        "key_column": "order_id",
        "unique_key": False,
        "load": {"mode": "incremental", "watermark_column": "order_date", "state_file": "state/x.json"},
        "output": {"directory": "output"},
    }
    cfg.update(overrides)
    return cfg


class ConfigValidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.src = self.root / "orders.csv"
        write_csv(self.src, GOOD_ROWS)

    def tearDown(self):
        self.tmp.cleanup()

    def test_valid_csv_config_loads(self):
        cfg = validate_config(csv_config(self.src, self.root / "out"))
        self.assertEqual(cfg.source_type, "csv")
        self.assertEqual(cfg.load_mode, "full")

    def test_unknown_top_level_key_rejected(self):
        raw = csv_config(self.src, self.root / "out")
        raw["script"] = "print('x')"
        with self.assertRaisesRegex(ConfigError, "script"):
            validate_config(raw)

    def test_inline_password_rejected(self):
        raw = pg_config()
        raw["source"]["password"] = "hunter2"
        with mock.patch.dict(os.environ, {"TEST_ORDERS_PG_PASSWORD": "x"}), self.assertRaisesRegex(
            ConfigError, "password"
        ):
            validate_config(raw)

    def test_incremental_without_watermark_rejected(self):
        raw = csv_config(self.src, self.root / "out", mode="incremental", state_file="s.json")
        with self.assertRaises(ConfigError):
            validate_config(raw)

    def test_unknown_type_rejected(self):
        raw = csv_config(self.src, self.root / "out")
        raw["columns"]["quantity"] = "eval"
        with self.assertRaises(ConfigError):
            validate_config(raw)

    def test_bad_table_identifier_rejected(self):
        raw = pg_config()
        raw["source"]["table"] = "orders; DROP TABLE x"
        with mock.patch.dict(os.environ, {"TEST_ORDERS_PG_PASSWORD": "x"}), self.assertRaises(ConfigError):
            validate_config(raw)

    def test_missing_password_env_var_rejected(self):
        env = {k: v for k, v in os.environ.items() if k != "TEST_ORDERS_PG_PASSWORD"}
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(ConfigError, "TEST_ORDERS_PG_PASSWORD"):
                validate_config(pg_config())

    def test_missing_csv_file_rejected(self):
        raw = csv_config(self.root / "nope.csv", self.root / "out")
        with self.assertRaisesRegex(ConfigError, "nope.csv"):
            validate_config(raw)

    def test_all_errors_reported_together(self):
        raw = csv_config(self.src, self.root / "out")
        raw["columns"]["quantity"] = "eval"
        raw["load"] = {"mode": "weird"}
        with self.assertRaises(ConfigError) as ctx:
            validate_config(raw)
        self.assertGreaterEqual(str(ctx.exception).count("\n"), 1)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.src = self.root / "orders.csv"
        self.out = self.root / "out"

    def tearDown(self):
        self.tmp.cleanup()

    def test_full_load_writes_all_valid_rows(self):
        write_csv(self.src, GOOD_ROWS)
        stats = run_ingestion(validate_config(csv_config(self.src, self.out)))
        self.assertEqual((stats.read, stats.valid, stats.rejected), (3, 3, 0))
        with (self.out / "orders_valid.csv").open(encoding="utf-8") as fh:
            written = list(csv.reader(fh))
        self.assertEqual(written[0], HEADER)
        self.assertEqual(len(written), 4)

    def test_bad_rows_are_rejected_and_not_written(self):
        rows = GOOD_ROWS + [
            ["", "504", "2004", "2024-01-08", "1", "5.00", "COMPLETED"],
            ["1005", "505", "2005", "2024-01-09", "1", "abc", "COMPLETED"],
        ]
        write_csv(self.src, rows)
        stats = run_ingestion(validate_config(csv_config(self.src, self.out)))
        self.assertEqual((stats.read, stats.valid, stats.rejected), (5, 3, 2))
        content = (self.out / "orders_valid.csv").read_text(encoding="utf-8")
        self.assertNotIn("abc", content)

    def test_unique_key_rejects_duplicates(self):
        write_csv(self.src, GOOD_ROWS + [GOOD_ROWS[0]])
        raw = csv_config(self.src, self.out)
        raw["unique_key"] = True
        stats = run_ingestion(validate_config(raw))
        self.assertEqual((stats.valid, stats.rejected), (3, 1))

    def test_missing_header_column_fails(self):
        write_csv(self.src, [r[:-1] for r in GOOD_ROWS], header=HEADER[:-1])
        with self.assertRaisesRegex(IngestError, "status"):
            run_ingestion(validate_config(csv_config(self.src, self.out)))

    def test_incremental_csv_second_run_skips_seen_rows(self):
        write_csv(self.src, GOOD_ROWS)
        state = self.root / "state" / "orders.json"
        raw = csv_config(self.src, self.out, mode="incremental", watermark_column="order_date", state_file=str(state))
        cfg = validate_config(raw)

        first = run_ingestion(cfg)
        self.assertEqual((first.valid, first.skipped), (3, 0))
        self.assertTrue(state.exists())
        self.assertEqual(json.loads(state.read_text(encoding="utf-8"))["watermark"], "2024-01-07")

        second = run_ingestion(cfg)
        self.assertEqual((second.valid, second.skipped), (0, 3))

        write_csv(self.src, GOOD_ROWS + [["1004", "504", "2004", "2024-01-09", "1", "5.00", "COMPLETED"]])
        third = run_ingestion(cfg)
        self.assertEqual((third.valid, third.skipped), (1, 3))

    def test_failed_run_does_not_advance_watermark(self):
        write_csv(self.src, GOOD_ROWS)
        state = self.root / "state" / "orders.json"
        cfg = validate_config(
            csv_config(self.src, self.out, mode="incremental", watermark_column="order_date", state_file=str(state))
        )
        with mock.patch("ingestion.pipeline.parse_row", side_effect=RuntimeError("boom")), self.assertRaises(RuntimeError):
            run_ingestion(cfg)
        self.assertFalse(state.exists())
        self.assertEqual(list(self.out.glob("*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
