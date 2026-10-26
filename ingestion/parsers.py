"""Type registry and row-level validation.

Parsers are fixed functions selected by name from configuration. Configuration
never supplies code.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Mapping, Optional


class RowError(ValueError):
    """A single record failed validation and should be rejected."""


def _parse_int(raw: str) -> int:
    return int(raw.strip())


def _parse_date(raw: str) -> date:
    return date.fromisoformat(raw.strip())


def _parse_text(raw: str) -> str:
    return raw.strip()


def _parse_amount(raw: str) -> Decimal:
    try:
        value = Decimal(raw.strip())
    except InvalidOperation as exc:
        raise ValueError("amount is not a number") from exc
    if not value.is_finite():
        raise ValueError("amount must be a finite number")
    if value < 0:
        raise ValueError("amount must not be negative")
    return value


TYPE_PARSERS: Dict[str, Callable[[str], Any]] = {
    "int": _parse_int,
    "text": _parse_text,
    "date": _parse_date,
    "amount": _parse_amount,
}

# Types that can serve as an incremental watermark (they have a meaningful order).
ORDERABLE_TYPES = ("int", "date", "amount")


def parse_value(type_name: str, raw: str) -> Any:
    return TYPE_PARSERS[type_name](raw)


def parse_row(row: Mapping[Optional[str], Any], columns: Mapping[str, str]) -> Dict[str, Any]:
    """Validate and type-cast one record. Raises RowError if the record must be rejected."""
    if None in row:  # csv.DictReader stores surplus fields under the None key
        raise RowError("row has more fields than the header")

    parsed: Dict[str, Any] = {}
    for column, type_name in columns.items():
        value = row.get(column)
        if value is None or not str(value).strip():
            raise RowError(f"missing value for '{column}'")
        try:
            parsed[column] = parse_value(type_name, str(value))
        except (ValueError, ArithmeticError) as exc:
            raise RowError(f"invalid '{column}' value {value!r}: {exc}") from exc
    return parsed
