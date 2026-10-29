from pathlib import Path
import argparse
import json

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "status",
]


ALLOWED_STATUSES = {"COMPLETED", "CANCELLED", "PENDING"}

def validate_orders(
    orders: pd.DataFrame,
    valid_customer_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return accepted and rejected rows without silently dropping records."""


    missing_columns = set(REQUIRED_COLUMNS) - set(orders.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )


    df = orders[REQUIRED_COLUMNS].copy().reset_index(drop=True)


    # Track source data row numbers (header is row 1).
    df.insert(0, "source_row_number", range(2, len(df) + 2))


    for col in ["order_id", "customer_id", "product_id", "status"]:
        df[col] = df[col].astype("string").str.strip()


    df["status"] = df["status"].str.upper()


    # Preserve original values in the output, but use parsed values
    # to determine whether dates and numbers are valid.
    parsed_date = pd.to_datetime(df["order_date"], errors="coerce")
    quantity = pd.to_numeric(df["quantity"], errors="coerce")
    price = pd.to_numeric(df["unit_price"], errors="coerce")


    reasons = [[] for _ in range(len(df))]


    def flag(mask: pd.Series, reason: str) -> None:
        for index in df.index[mask.fillna(True)]:
            reasons[index].append(reason)


    for col in REQUIRED_COLUMNS:
        flag(df[col].isna() | df[col].eq(""), f"MISSING_{col.upper()}")


    # Reject every occurrence of a duplicated non-empty order ID.
    has_order_id = df["order_id"].notna() & df["order_id"].ne("")
    duplicate_ids = df.loc[has_order_id, "order_id"].duplicated(keep=False)
    duplicate_index = duplicate_ids[duplicate_ids].index
    duplicate_mask = df.index.isin(duplicate_index)
    flag(pd.Series(duplicate_mask, index=df.index), "DUPLICATE_ORDER_ID")


    flag(parsed_date.isna(), "INVALID_ORDER_DATE")
    flag(quantity.isna(), "INVALID_QUANTITY")
    flag(quantity.notna() & (quantity % 1 != 0), "NON_INTEGER_QUANTITY")
    flag(quantity.notna() & (quantity <= 0), "NON_POSITIVE_QUANTITY")
    flag(price.isna(), "INVALID_UNIT_PRICE")
    flag(price.notna() & (price < 0), "NEGATIVE_UNIT_PRICE")


    flag(
        df["status"].notna() & ~df["status"].isin(ALLOWED_STATUSES),
        "INVALID_STATUS",
    )


    flag(
        df["customer_id"].notna()
        & df["customer_id"].ne("")
        & ~df["customer_id"].isin(valid_customer_ids),
        "UNKNOWN_CUSTOMER_ID",
    )


    df["rejection_reason"] = [
        ";".join(dict.fromkeys(row_reasons))
        for row_reasons in reasons
    ]


    rejected_mask = df["rejection_reason"].ne("")
    accepted = df.loc[~rejected_mask].copy()
    rejected = df.loc[rejected_mask].copy()


    # Parse valid output types.
    accepted["order_date"] = parsed_date.loc[accepted.index].dt.strftime("%Y-%m-%d")
    accepted["quantity"] = quantity.loc[accepted.index].astype("int64")
    accepted["unit_price"] = price.loc[accepted.index].round(2)


    # Rejected records retain their values for investigation.
    return accepted, rejected




def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        default="data/samples/orders_quality.csv",
        help="Path to input orders CSV, relative to project root or absolute",
    )
    args = parser.parse_args()


    source = Path(args.source)
    if not source.is_absolute():
        source = ROOT / source


    customers_path = ROOT / "data/samples/customers.csv"
    output_dir = ROOT / "data/processed"
    logs_dir = ROOT / "logs"


    if not source.exists():
        raise FileNotFoundError(f"Input file not found: {source}")
    if not customers_path.exists():
        raise FileNotFoundError(
            f"Customer reference file not found: {customers_path}"
        )


    customers = pd.read_csv(
        customers_path, dtype={"customer_id": "string"}
    )
    if "customer_id" not in customers.columns:
        raise ValueError("Customer file must contain customer_id")


    valid_customer_ids = set(
        customers["customer_id"].dropna().str.strip()
    )


    orders = pd.read_csv(
        source,
        dtype={
            "order_id": "string",
            "customer_id": "string",
            "product_id": "string",
            "status": "string",
        },
        keep_default_na=True,
    )


    accepted, rejected = validate_orders(orders, valid_customer_ids)


    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)


    accepted_path = output_dir / "orders_validated.csv"
    rejected_path = output_dir / "orders_rejected.csv"
    report_path = logs_dir / "quality_report.json"


    accepted.to_csv(accepted_path, index=False)
    rejected.to_csv(rejected_path, index=False)


    report = {
        "source": str(source),
        "rows_read": len(orders),
        "rows_accepted": len(accepted),
        "rows_rejected": len(rejected),
        "rejection_reason_counts": (
            rejected["rejection_reason"]
            .str.split(";")
            .explode()
            .value_counts()
            .to_dict()
        ),
        "accepted_file": str(accepted_path),
        "rejected_file": str(rejected_path),
    }


    report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )


    print(json.dumps(report, indent=2))




if __name__ == "__main__":
    main()
