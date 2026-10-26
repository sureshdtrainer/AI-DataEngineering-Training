"""Shared ingestion pipeline: read, validate, filter, write, checkpoint, log.

Source-specific behaviour lives in `sources.py`. Everything here applies to any
source type.
"""

from __future__ import annotations

import csv
import json
import logging
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Set, Tuple

from .config import IngestConfig
from .errors import IngestError
from .parsers import RowError, parse_row, parse_value
from .sources import RecordIterator, read_csv, read_postgres

LOGGER = logging.getLogger("ingestion")
OUTPUT_ENCODING = "utf-8"


@dataclass
class RunStats:
    read: int = 0
    valid: int = 0
    rejected: int = 0
    skipped: int = 0


def setup_logging(level: str) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def log_summary(name: str, stats: RunStats) -> None:
    LOGGER.info(
        "[%s] records read=%d valid=%d rejected=%d skipped=%d",
        name,
        stats.read,
        stats.valid,
        stats.rejected,
        stats.skipped,
    )


def load_watermark(state_file: Path) -> Optional[str]:
    """Return the last stored watermark, or None on the first run."""
    if not state_file.exists():
        return None
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IngestError(f"cannot read state file {state_file}: {exc}") from exc
    value = data.get("watermark") if isinstance(data, dict) else None
    if not isinstance(value, str):
        raise IngestError(f"state file {state_file} has no valid 'watermark' value")
    return value


def save_watermark(state_file: Path, watermark: str) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_file.with_name(state_file.name + ".tmp")
    payload = {"watermark": watermark, "updated_at": datetime.now(timezone.utc).isoformat()}
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp_path.replace(state_file)


def _output_path(cfg: IngestConfig, started_at: datetime) -> Path:
    if cfg.load_mode == "full":
        return cfg.output_dir / cfg.output_file
    # Each incremental run writes its own batch file so earlier batches are never overwritten.
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    return cfg.output_dir / f"{Path(cfg.output_file).stem}_{stamp}_{uuid.uuid4().hex[:8]}.csv"


def _source_records(cfg: IngestConfig, last_value: Any) -> RecordIterator:
    if cfg.source_type == "csv":
        return read_csv(Path(cfg.source["path"]), cfg.columns)
    return read_postgres(cfg.source, cfg.columns, cfg.watermark_column, last_value)


def _last_watermark_value(cfg: IngestConfig) -> Tuple[Optional[str], Any]:
    if cfg.load_mode != "incremental":
        return None, None
    last_raw = load_watermark(cfg.state_file)
    if last_raw is None:
        return None, None
    try:
        return last_raw, parse_value(cfg.columns[cfg.watermark_column], last_raw)
    except (ValueError, ArithmeticError) as exc:
        raise IngestError(f"state file has invalid watermark {last_raw!r}: {exc}") from exc


def run_ingestion(cfg: IngestConfig) -> RunStats:
    started_at = datetime.now(timezone.utc)
    watermark = cfg.watermark_column
    last_raw, last_value = _last_watermark_value(cfg)
    if cfg.load_mode == "incremental":
        LOGGER.info("[%s] incremental run; last watermark: %s", cfg.name, last_raw or "none (first run)")
    else:
        LOGGER.info("[%s] full load", cfg.name)

    output_path = _output_path(cfg, started_at)
    tmp_path = output_path.with_name(output_path.name + ".tmp")
    stats = RunStats()
    max_value: Any = None
    seen_keys: Set[Any] = set()

    try:
        cfg.output_dir.mkdir(parents=True, exist_ok=True)
        with tmp_path.open("w", newline="", encoding=OUTPUT_ENCODING) as dst:
            writer = csv.writer(dst)
            writer.writerow(list(cfg.columns))

            for reference, row in _source_records(cfg, last_value):
                stats.read += 1
                try:
                    parsed = parse_row(row, cfg.columns)
                except RowError as exc:
                    stats.rejected += 1
                    LOGGER.warning("[%s] rejected %s: %s", cfg.name, reference, exc)
                    continue

                if watermark is not None and last_value is not None and parsed[watermark] <= last_value:
                    stats.skipped += 1
                    continue

                if cfg.unique_key:
                    key = parsed[cfg.key_column]
                    if key in seen_keys:
                        stats.rejected += 1
                        LOGGER.warning("[%s] rejected %s: duplicate %s %s", cfg.name, reference, cfg.key_column, key)
                        continue
                    seen_keys.add(key)

                if watermark is not None and (max_value is None or parsed[watermark] > max_value):
                    max_value = parsed[watermark]

                writer.writerow(str(parsed[column]) for column in cfg.columns)
                stats.valid += 1

        tmp_path.replace(output_path)
    except UnicodeDecodeError as exc:
        raise IngestError(f"cannot decode input as UTF-8: {exc}") from exc
    except csv.Error as exc:
        raise IngestError(f"CSV parsing error: {exc}") from exc
    except OSError as exc:
        raise IngestError(f"file I/O error during ingestion: {exc}") from exc
    finally:
        with suppress(OSError):
            tmp_path.unlink(missing_ok=True)

    # Checkpoint only after the output file is in place, so a failed run never advances the watermark.
    if cfg.load_mode == "incremental" and max_value is not None:
        save_watermark(cfg.state_file, str(max_value))
        LOGGER.info("[%s] watermark advanced to %s", cfg.name, max_value)

    LOGGER.info("[%s] output written to %s", cfg.name, output_path)
    log_summary(cfg.name, stats)
    return stats
