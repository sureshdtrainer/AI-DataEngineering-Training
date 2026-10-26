"""Source readers.

Each reader yields `(reference, raw_row)` pairs, where `reference` identifies the
record in log messages. Adding a source type means adding a reader here and a
branch in the pipeline's source selector.
"""

from __future__ import annotations

import csv
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence, Tuple

from .errors import IngestError

LOGGER = logging.getLogger("ingestion")

CSV_ENCODING = "utf-8-sig"  # strips a byte-order mark if present
PG_FETCH_SIZE = 1000
PG_CONNECT_TIMEOUT_SECONDS = 10

RecordIterator = Iterator[Tuple[str, Dict[str, Any]]]


def validate_csv_header(fieldnames: Optional[Sequence[str]], columns: Mapping[str, str]) -> None:
    if not fieldnames:
        raise IngestError("input file is empty or has no header row")
    missing = [column for column in columns if column not in fieldnames]
    if missing:
        raise IngestError(
            f"missing required column(s): {', '.join(missing)}; found: {', '.join(fieldnames)}"
        )
    extra = [column for column in fieldnames if column not in columns]
    if extra:
        LOGGER.warning("Ignoring unexpected column(s): %s", ", ".join(extra))


def read_csv(path: Path, columns: Mapping[str, str]) -> RecordIterator:
    with path.open(newline="", encoding=CSV_ENCODING) as handle:
        reader = csv.DictReader(handle)
        validate_csv_header(reader.fieldnames, columns)
        for row in reader:
            yield f"line {reader.line_num}", row


def read_postgres(
    source: Mapping[str, Any],
    columns: Mapping[str, str],
    watermark: Optional[str],
    last_value: Any,
) -> RecordIterator:
    """Stream rows from a PostgreSQL table, filtering on the watermark when one is given.

    Identifiers are quoted with psycopg's sql module. Values are passed as bound
    parameters. The password is read from the environment variable named in
    `password_env`.
    """
    try:
        import psycopg
        from psycopg import sql
    except ImportError as exc:
        raise IngestError("PostgreSQL sources require psycopg: pip install \"psycopg[binary]\"") from exc

    env_name = source["password_env"]
    password = os.environ.get(env_name)
    if not password:
        raise IngestError(f"environment variable '{env_name}' is not set")

    select_list = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
    query = sql.SQL("SELECT {} FROM {}").format(
        select_list, sql.Identifier(source["schema"], source["table"])
    )
    params: Tuple[Any, ...] = ()
    if watermark is not None and last_value is not None:
        watermark_id = sql.Identifier(watermark)
        query = sql.SQL("{} WHERE {} > %s ORDER BY {}").format(query, watermark_id, watermark_id)
        params = (last_value,)

    column_names = list(columns)
    location = f"{source['schema']}.{source['table']}"
    try:
        with psycopg.connect(
            host=source["host"],
            port=source["port"],
            dbname=source["database"],
            user=source["user"],
            password=password,
            sslmode=source["sslmode"],
            connect_timeout=PG_CONNECT_TIMEOUT_SECONDS,
        ) as conn:
            conn.read_only = True
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                index = 0
                while True:
                    batch = cursor.fetchmany(PG_FETCH_SIZE)
                    if not batch:
                        break
                    for values in batch:
                        index += 1
                        yield f"row {index}", dict(zip(column_names, values))
    except psycopg.Error as exc:
        raise IngestError(f"PostgreSQL error reading {location}: {exc}") from exc
