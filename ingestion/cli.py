"""Command-line entry point: validate configuration, then run the ingestion."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional, Sequence

from .config import load_config
from .errors import ConfigError, IngestError
from .pipeline import run_ingestion, setup_logging

LOGGER = logging.getLogger("ingestion")
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Configuration-driven data ingestion.")
    parser.add_argument("--config", required=True, type=Path, help="Path to the JSON ingestion config.")
    parser.add_argument(
        "--validate-only", action="store_true", help="Validate the configuration and exit without ingesting."
    )
    parser.add_argument("--log-level", default="INFO", choices=LOG_LEVELS, help="Logging verbosity.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        LOGGER.error("%s", exc)
        return 1

    if args.validate_only:
        LOGGER.info("Configuration '%s' is valid (source=%s, mode=%s)", cfg.name, cfg.source_type, cfg.load_mode)
        return 0

    try:
        run_ingestion(cfg)
    except IngestError as exc:
        LOGGER.error("[%s] ingestion failed: %s", cfg.name, exc)
        return 1
    return 0
