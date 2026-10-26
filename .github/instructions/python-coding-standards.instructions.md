---
description: "Use when writing or editing Python code: modules, functions, classes, error handling, logging, type hints, file I/O, and the ingestion package. Covers project Python coding standards."
applyTo: "**/*.py"
---
# Python Coding Standards

- Start every module with a docstring that states its purpose. Put `from __future__ import annotations` directly after it.
- Group imports: standard library, then local relative imports (`from .config import ...`). Keep one import per line for `from` imports that span several names only when the line gets long.
- Use `typing` generics (`Optional`, `Dict`, `Tuple`, `Set`, `Mapping`, `Callable`) to match the existing code, rather than `X | None` or `dict[...]`.
- Annotate every function signature (parameters and return type). Use `Any` only when the type is truly dynamic.
- Use `@dataclass` for simple result or state holders, as in `RunStats`.
- Use a module-level `LOGGER = logging.getLogger("<name>")`. Do not use `print` in library code.
- Log with `%`-style arguments (`LOGGER.info("[%s] ...", name)`), not f-strings.
- Prefix log messages with the source or pipeline name in brackets, for example `[orders_csv]`.
- Raise the package exceptions from `errors.py` (`IngestError`, `ConfigError`) for fatal problems. Chain the original exception with `from exc`.
- Use `RowError` (a `ValueError` subclass) for a single bad record that should be rejected, not stopped.
- Catch specific exception types. Never use a bare `except:`.
- Use `contextlib.suppress(...)` for cleanup that may safely fail, as in the `finally` block of `run_ingestion`.
- Write files with an explicit `encoding="utf-8"`. For CSV, open with `newline=""`.
- Write output atomically: write to a `.tmp` path, then `Path.replace()` it into place. Save checkpoints only after output succeeds.
- Use `pathlib.Path` for file paths, not `os.path` string handling.
- Use `datetime.now(timezone.utc)` for timestamps. Never use naive datetimes.
- Keep lines at or under about 120 characters, matching the existing code.
- Put source-specific logic in `sources.py` and shared pipeline logic in `pipeline.py`. Do not let configuration supply code: parser types are fixed functions looked up by name.
- Add or update tests in `tests/` (for example `tests/test_ingestion.py`) when changing behavior in the `ingestion` package.
