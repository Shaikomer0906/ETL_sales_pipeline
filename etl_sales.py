import pandas as pd
import re
import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

INPUT_FILE   = "data/sales.csv"
ERROR_DIR    = "errors"
VALID_REGIONS = {"north", "south", "east", "west"}
os.makedirs(ERROR_DIR, exist_ok=True)


def extract(filepath):
    log.info(f"Extracting: {filepath}")
    df = pd.read_csv(filepath, dtype=str)
    df.columns = df.columns.str.strip().str.lower()
    log.info(f"Rows ingested: {len(df)}")
    return df


def transform(df):
    before = len(df)
    df = df.drop_duplicates()
    log.info(f"Duplicates removed: {before - len(df)}")

    df["product_name"]      = df["product_name"].str.strip().str.title()
    df["region"]            = df["region"].str.strip().str.lower()
    df["salesperson_email"] = df["salesperson_email"].str.strip().str.lower()
    df["sale_id"]           = df["sale_id"].str.strip()

    def parse_date(val):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                return pd.to_datetime(val, format=fmt)
            except (ValueError, TypeError):
                continue
        return pd.NaT

    df["sale_date"]  = df["sale_date"].apply(parse_date)
    df["quantity"]   = pd.to_numeric(df["quantity"],   errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    def validate_row(row):
        reasons = []

        if not row["sale_id"] or pd.isna(row["sale_id"]):
            reasons.append("missing sale_id")

        if not row["product_name"] or pd.isna(row["product_name"]):
            reasons.append("missing product_name")

        if not row["region"] or pd.isna(row["region"]):
            reasons.append("missing region")

        if row["region"] not in VALID_REGIONS:
            reasons.append(f"invalid region: {row['region']}")

        if pd.isna(row["sale_date"]):
            reasons.append("invalid or missing sale_date")

        if pd.isna(row["quantity"]) or row["quantity"] <= 0:
            reasons.append("invalid quantity — must be a number greater than 0")

        if pd.isna(row["unit_price"]) or row["unit_price"] <= 0:
            reasons.append("invalid unit_price — must be a number greater than 0")

        if not re.match(r"^[\w\.-]+@[\w\.-]+\.\w{2,}$", str(row["salesperson_email"])):
            reasons.append("invalid salesperson_email")

        return reasons

    valid_rows   = []
    invalid_rows = []

    for _, row in df.iterrows():
        issues = validate_row(row)
        if issues:
            row_dict = row.to_dict()
            row_dict["_error_reasons"] = "; ".join(issues)
            invalid_rows.append(row_dict)
        else:
            valid_rows.append(row.to_dict())

    df_valid   = pd.DataFrame(valid_rows)
    df_invalid = pd.DataFrame(invalid_rows)

    log.info(f"Valid rows:   {len(df_valid)}")
    log.info(f"Invalid rows: {len(df_invalid)}")

    return df_valid, df_invalid


def load(df, engine):
    if df.empty:
        log.warning("No valid rows to load.")
        return 0

    cols = ["sale_id","product_name","region","sale_date",
            "quantity","unit_price","salesperson_email"]
    df = df[cols]
    loaded = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                conn.execute(text("""
                    INSERT INTO sales_clean
                        (sale_id, product_name, region, sale_date,
                         quantity, unit_price, salesperson_email)
                    VALUES
                        (:sale_id, :product_name, :region, :sale_date,
                         :quantity, :unit_price, :salesperson_email)
                    ON CONFLICT (sale_id) DO UPDATE SET
                        product_name      = EXCLUDED.product_name,
                        region            = EXCLUDED.region,
                        sale_date         = EXCLUDED.sale_date,
                        quantity          = EXCLUDED.quantity,
                        unit_price        = EXCLUDED.unit_price,
                        salesperson_email = EXCLUDED.salesperson_email,
                        loaded_at         = NOW()
                """), row.to_dict())
                loaded += 1
            except Exception as e:
                log.error(f"Row insert failed for sale_id={row['sale_id']}: {e}")

    log.info(f"Rows loaded: {loaded}")
    return loaded


def run_pipeline():
    start = datetime.now()
    log.info("Pipeline started")

    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    raw_df             = extract(INPUT_FILE)
    clean_df, error_df = transform(raw_df)
    loaded             = load(clean_df, engine)

    if not error_df.empty:
        error_path = os.path.join(
            ERROR_DIR,
            f"errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        error_df.to_csv(error_path, index=False)
        log.info(f"Error report: {error_path}")

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"Pipeline complete | Loaded: {loaded} rows | Time: {elapsed:.2f}s")


if __name__ == "__main__":
    run_pipeline()