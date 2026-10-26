---
name: data-validation
description: 'Verify and validate tabular data for the reference pipeline: not-null, duplicate, type, range, accepted-value, referential-integrity, and row-reconciliation checks. Use when asked to validate, verify, profile, or check data quality, duplicates, nulls, or bad rows in CSV, PostgreSQL, or pipeline output.'
argument-hint: 'Dataset or file to validate, for example data/samples/orders.csv'
---

# Data Validation

## When to Use
- Check a source file or table before or after an ingestion run.
- Verify pipeline output (for example under `output/`) against its input.
- Add or review data quality rules for a dataset in `configs/` or `tests/`.

## Inputs to Confirm
1. Dataset path or table, and its schema (column names and types). Read the matching config in `configs/` first.
2. Key column(s) that identify a record (for example `order_id`).
3. Business rules, such as allowed `status` values. Check `docs/`, `metadata/`, and `config/` first. If none exist, ask the user before inventing a rule.

## Check Catalog

| Check | Rule | Example |
|-------|------|---------|
| Schema | All expected columns exist; no required column is missing | `status` present in `orders.csv` |
| Not null | Required columns have a non-blank value | `order_id`, `order_date` |
| Type | Each value parses to its declared type (`int`, `date`, `amount`, `text`) | `order_date` is ISO `YYYY-MM-DD` |
| Duplicate key | Key column(s) are unique across the dataset | `order_id` unique |
| Duplicate row | No fully identical rows | Same 7 values appear twice |
| Range | Numeric values within business bounds | `quantity` > 0, `unit_price` >= 0 and finite |
| Accepted values | Categorical values come from an approved list | `status` in {COMPLETED, CANCELLED} (sample data) |
| Referential integrity | Foreign keys exist in the parent dataset | `customer_id` in `data/raw/customers.csv` |
| Reconciliation | Input rows = valid + rejected + skipped; output rows = valid | `read=5, valid=3, rejected=2` |
| Watermark | Incremental watermark is orderable and advances only after output is written | `state` file matches max written value |

Type, not-null, and duplicate-key checks already run inside `ingestion/parsers.py` and `ingestion/pipeline.py`. Do not reimplement them silently. Use this catalog to confirm they run, and to add the checks that are missing (range, accepted values, referential integrity).

## Procedure
1. Read the dataset header and the config. Confirm the expected columns and types.
2. Run the schema, not-null, and type checks. Record counts of failing rows per column, with at least one example each.
3. Run duplicate-key and duplicate-row checks. Report the duplicated key values.
4. Run range and accepted-value checks only against rules confirmed in step 1 of Inputs to Confirm.
5. Run referential-integrity checks by joining to the parent dataset on its key. Report orphan keys.
6. Reconcile counts: input rows must equal valid + rejected + skipped. Output rows must equal valid rows.
7. Report results (see Output Format). Do not modify source data.

## Implementation Rules
- Put new checks in code as fixed functions (for example in `ingestion/`), selected by name from config. Configs stay data only.
- Reject bad rows with a clear reason. Do not drop them silently.
- Write each new check's behavior as tests in `tests/` with positive, negative, and edge cases (empty file, all rows bad, nulls, whitespace-only values, duplicate at the end of the file).
- Run `pytest` from the repository root after changes.

## Output Format
```
Dataset: <path or table>
Rows read: <n>   valid: <n>   rejected: <n>   skipped: <n>
| Check | Status | Failing rows | Example |
|-------|--------|--------------|---------|
| Not null: order_id | FAIL | 1 | line 7 |
Verified: <what ran>   Not verified: <what did not run, and why>
```

## Quality Criteria
- Every failing check reports a count and at least one example.
- Every reported number comes from an actual run or query, not an estimate.
- The report states what was not verified.
