# AI-Assisted Data Engineering Training: Project Instructions

## Project Purpose

Training workspace for building a reference data pipeline: ingestion, transformation, quality checks, and orchestration. The stack is Python, PySpark, Airflow, dbt Core, PostgreSQL, and Docker Compose. Code must stay vendor-neutral and easy to map to enterprise platforms such as Databricks, ADF, Glue, and Snowflake.

## Repository Layout

- `ingestion/`: configuration-driven ingestion package (`config.py`, `sources.py`, `parsers.py`, `pipeline.py`, `errors.py`, `cli.py`). Run with `python -m ingestion --config <file>`.
- `configs/`: JSON ingestion configs. `orders_csv.json` is the CSV example. `orders_postgres.example.json` is a template.
- `dags/`: Airflow DAGs. Mounted at `/opt/airflow/dags`.
- `scripts/`: standalone scripts, such as `spark_smoke_test.py`.
- `data/raw/`, `data/samples/`: input data. `data/processed/`: pipeline outputs.
- `tests/`: pytest tests.
- `labs/module-XX/README.md`: training lab instructions. Read the relevant lab before changing lab-related code.
- `docs/`, `metadata/`, `config/`: documentation and metadata. Check these for business rules before inventing them.
- `docker-compose.yml`, `Dockerfile.airflow`, `requirements.txt`: environment definition.

## Working Rules

- Read the relevant lab README and existing code before generating new pipeline code.
- Prefer configuration-driven designs over hard-coded logic. Configs are data only and must never be executed.
- Follow [python-coding-standards.instructions.md](instructions/python-coding-standards.instructions.md) for all Python files.
- Make minimal, focused changes. Do not refactor unrelated code.
- Keep changes runnable in the Docker environment defined in `docker-compose.yml`.

## Data Engineering Standards

- Validate data at the boundary: check types, nulls, duplicates, and business rules. Reject bad rows with clear reasons rather than failing silently.
- Support full and incremental loads. Incremental watermarks must be orderable types (`int`, `date`, `amount`).
- Advance checkpoints or watermarks only after output is written successfully.
- Write outputs atomically (temporary file, then rename). Never overwrite earlier batch outputs.
- Preserve source-to-target column mappings and document any transformation rule you add.
- Treat AI-generated SQL and PySpark as untrusted until tested against expected business logic.
- Add or update tests for every behavior change, including positive, negative, and edge cases.

## Airflow Standards

- Use `dag_id` values that are descriptive and match the lab or pipeline they belong to.
- Set `catchup=False` unless backfill is explicitly required. Set explicit `retries` and `retry_delay`.
- Keep DAG files light: import heavy libraries (pandas, pyspark) inside task callables, as `training_smoke_dag.py` does.
- Tasks must be idempotent so that a rerun does not duplicate data.
- Dependencies between ingestion, transformation, and quality validation must be explicit.

## Security and Secrets

- Never hard-code passwords, tokens, or keys in code or config files. Reference them by environment variable name (for example, `password_env`).
- The credentials in `docker-compose.yml` are for local training only. Do not reuse them outside this environment, and do not copy them into production-style examples.
- Do not commit files under `logs/` or generated `output/` directories.

## Testing and Validation

- Run `pytest` from the repository root for changes in `ingestion/` or `tests/`.
- Validate configs with `python -m ingestion --config <file> --validate-only` before running an ingestion.
- Do not claim code works until it has been executed or tested. State clearly what was not verified.

## Communication

- Keep explanations short and practical. Training participants are experienced data engineers, so skip basic concepts.
- When suggesting AI-generated work, explain how to validate it.
