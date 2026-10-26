"""Load and validate JSON ingestion configuration.

Configuration is data only. Every value is checked against fixed rules and is
never executed. Credentials are referenced by environment-variable name
(`password_env`) and are never stored in the file.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .errors import ConfigError
from .parsers import ORDERABLE_TYPES, TYPE_PARSERS

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
ENV_VAR_RE = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")
SOURCE_TYPES = ("csv", "postgresql")
LOAD_MODES = ("full", "incremental")
SSL_MODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")
TOP_LEVEL_KEYS = {"name", "source", "columns", "key_column", "unique_key", "load", "output"}
DEFAULT_PG_PORT = 5432
DEFAULT_PG_SCHEMA = "public"
DEFAULT_PG_SSLMODE = "require"


@dataclass(frozen=True)
class IngestConfig:
    name: str
    source_type: str
    source: Dict[str, Any]
    columns: Dict[str, str]
    key_column: str
    unique_key: bool
    load_mode: str
    watermark_column: Optional[str]
    state_file: Optional[Path]
    output_dir: Path
    output_file: str


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _check_keys(
    section: Dict[str, Any],
    where: str,
    allowed: Set[str],
    required: Set[str],
    errors: List[str],
) -> None:
    for key in section:
        if key not in allowed:
            errors.append(f"{where}: unknown key '{key}'")
    for key in sorted(required - set(section)):
        errors.append(f"{where}: missing required key '{key}'")


def _validate_source(section: Any, errors: List[str]) -> Dict[str, Any]:
    if not isinstance(section, dict):
        errors.append("'source' must be an object")
        return {}

    source_type = section.get("type")
    if source_type not in SOURCE_TYPES:
        errors.append(f"source.type must be one of: {', '.join(SOURCE_TYPES)}")
        return {}

    if source_type == "csv":
        _check_keys(section, "source", {"type", "path"}, {"type", "path"}, errors)
        path = section.get("path")
        if not _non_empty_str(path):
            errors.append("source.path must be a non-empty string")
        elif not Path(path).is_file():
            errors.append(f"source.path not found: {path}")
        return dict(section)

    if "password" in section:
        errors.append("source: do not store passwords in config; use 'password_env' instead")
    allowed = {"type", "host", "port", "database", "user", "password_env", "sslmode", "schema", "table"}
    required = {"type", "host", "database", "user", "password_env", "table"}
    _check_keys(section, "source", allowed, required, errors)

    for key in ("host", "database", "user"):
        if key in section and not _non_empty_str(section[key]):
            errors.append(f"source.{key} must be a non-empty string")

    port = section.get("port", DEFAULT_PG_PORT)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        errors.append("source.port must be an integer between 1 and 65535")

    schema = section.get("schema", DEFAULT_PG_SCHEMA)
    table = section.get("table")
    for label, value in (("schema", schema), ("table", table)):
        if not isinstance(value, str) or not IDENTIFIER_RE.match(value):
            errors.append(f"source.{label} must be a plain identifier (letters, digits, underscores)")

    sslmode = section.get("sslmode", DEFAULT_PG_SSLMODE)
    if sslmode not in SSL_MODES:
        errors.append(f"source.sslmode must be one of: {', '.join(SSL_MODES)}")

    env_name = section.get("password_env")
    if not isinstance(env_name, str) or not ENV_VAR_RE.match(env_name):
        errors.append("source.password_env must be an environment variable name such as ORDERS_PG_PASSWORD")
    elif not os.environ.get(env_name):
        errors.append(f"environment variable '{env_name}' is not set (required for source.password_env)")

    if importlib.util.find_spec("psycopg") is None:
        errors.append("PostgreSQL sources require the 'psycopg' package: pip install \"psycopg[binary]\"")

    return {
        "type": source_type,
        "host": section.get("host"),
        "port": port,
        "database": section.get("database"),
        "user": section.get("user"),
        "password_env": env_name,
        "sslmode": sslmode,
        "schema": schema,
        "table": table,
    }


def _validate_columns(section: Any, errors: List[str]) -> Dict[str, str]:
    if not isinstance(section, dict) or not section:
        errors.append("'columns' must be a non-empty object mapping column name to type")
        return {}
    for name, type_name in section.items():
        if not IDENTIFIER_RE.match(name):
            errors.append(f"columns.{name}: column name must be a plain identifier")
        if type_name not in TYPE_PARSERS:
            errors.append(
                f"columns.{name}: unknown type {type_name!r} (allowed: {', '.join(TYPE_PARSERS)})"
            )
    return dict(section)


def _validate_load(
    section: Any, columns: Dict[str, str], errors: List[str]
) -> Tuple[str, Optional[str], Optional[str]]:
    if not isinstance(section, dict):
        errors.append("'load' must be an object")
        return "full", None, None

    _check_keys(section, "load", {"mode", "watermark_column", "state_file"}, {"mode"}, errors)
    mode = section.get("mode")
    if mode not in LOAD_MODES:
        errors.append(f"load.mode must be one of: {', '.join(LOAD_MODES)}")
        return "full", None, None

    watermark = section.get("watermark_column")
    state_file = section.get("state_file")
    if mode == "incremental":
        if not _non_empty_str(watermark):
            errors.append("load.watermark_column is required for incremental mode")
        elif watermark not in columns:
            errors.append(f"load.watermark_column '{watermark}' is not one of the configured columns")
        elif columns[watermark] not in ORDERABLE_TYPES:
            errors.append(
                f"load.watermark_column must be an orderable type ({', '.join(ORDERABLE_TYPES)}), "
                f"not '{columns[watermark]}'"
            )
        if not _non_empty_str(state_file):
            errors.append("load.state_file is required for incremental mode")
        return mode, watermark, state_file

    if watermark is not None or state_file is not None:
        errors.append("load.watermark_column and load.state_file apply only to incremental mode")
    return mode, None, None


def _validate_output(section: Any, name: Any, errors: List[str]) -> Tuple[str, str]:
    if not isinstance(section, dict):
        errors.append("'output' must be an object")
        return "", ""

    _check_keys(section, "output", {"directory", "file_name"}, {"directory"}, errors)
    directory = section.get("directory")
    if not _non_empty_str(directory):
        errors.append("output.directory must be a non-empty string")

    file_name = section.get("file_name", f"{name}_valid.csv")
    if (
        not _non_empty_str(file_name)
        or Path(file_name).name != file_name
        or not file_name.lower().endswith(".csv")
    ):
        errors.append("output.file_name must be a plain file name ending in .csv")
    return directory or "", file_name or ""


def validate_config(raw: Any) -> IngestConfig:
    """Validate a parsed configuration object. Collects all problems before failing."""
    if not isinstance(raw, dict):
        raise ConfigError("configuration must be a JSON object")

    errors: List[str] = []
    _check_keys(raw, "config", TOP_LEVEL_KEYS, {"name", "source", "columns", "key_column", "load", "output"}, errors)

    name = raw.get("name")
    if not isinstance(name, str) or not IDENTIFIER_RE.match(name):
        errors.append("name must be a plain identifier (letters, digits, underscores)")

    source = _validate_source(raw.get("source"), errors)
    columns = _validate_columns(raw.get("columns"), errors)

    key_column = raw.get("key_column")
    if key_column not in columns:
        errors.append("key_column must be one of the configured columns")

    unique_key = raw.get("unique_key", False)
    if not isinstance(unique_key, bool):
        errors.append("unique_key must be true or false")

    load_mode, watermark, state_file = _validate_load(raw.get("load"), columns, errors)
    directory, file_name = _validate_output(raw.get("output"), name, errors)

    if errors:
        raise ConfigError("Invalid configuration:\n" + "\n".join(f"  - {error}" for error in errors))

    return IngestConfig(
        name=name,
        source_type=source["type"],
        source=source,
        columns=columns,
        key_column=key_column,
        unique_key=unique_key,
        load_mode=load_mode,
        watermark_column=watermark,
        state_file=Path(state_file) if state_file else None,
        output_dir=Path(directory),
        output_file=file_name,
    )


def load_config(path: Path) -> IngestConfig:
    if not path.is_file():
        raise ConfigError(f"configuration file not found: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"configuration is not valid JSON ({path}): line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise ConfigError(f"cannot read configuration {path}: {exc}") from exc
    return validate_config(raw)
