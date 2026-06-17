import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ------------------ Config ------------------
load_dotenv("config.env")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)

CUTOFF_DATE = "2025-01-02"    # delete records AFTER this date
DRY_RUN = True                # True = preview only, False = actually delete

tables = [
    {
        "schema_table": "old.assignment_monitoring_data",
        "submitted_at": '"submitted_at"',
        "backup_file": "deleted_old_after_2025-01-03.csv",
    },
    {
        "schema_table": '"raw".student_assignment',
        "submitted_at": "submitted_at",
        "backup_file": "deleted_raw_after_2025-01-03.csv",
    },
    {
        "schema_table": "intermediate.final_assignment",
        "submitted_at": "submitted_at",
        "backup_file": "deleted_final_after_2025-01-03.csv",
    }
]

# ------------ utility: clean control chars ------------
def clean_text(df):
    illegal = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).apply(lambda x: illegal.sub("", x))
    return df

# ------------ core ------------
def process_table(cfg):
    table = cfg["schema_table"]
    submitted_at = cfg["submitted_at"]
    backup_file = cfg["backup_file"]

    print(f"\n🔍 Checking table: {table}")
    print(f"   Cutoff date (delete AFTER): {CUTOFF_DATE}")
    print(f"   Mode: {'DRY RUN' if DRY_RUN else 'LIVE DELETE'}")

    # Boundary: >= next-day 00:00:00 UTC (i.e., strictly after the whole cutoff day)
    select_query = f"""
        WITH boundary AS (
            SELECT (DATE '{CUTOFF_DATE}' + INTERVAL '1 day') AS dt_utc_start
        )
        SELECT *
        FROM {table}, boundary
        WHERE {submitted_at} IS NOT NULL
          AND CAST({submitted_at} AS text) NOT IN ('NaN', '', 'null')
          AND ({submitted_at}::timestamptz AT TIME ZONE 'UTC') >= boundary.dt_utc_start;
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(select_query), conn)

    if df.empty:
        print(f"✅ No records found AFTER {CUTOFF_DATE} in {table}.")
        return

    print(f"🧮 Found {len(df)} records in {table} AFTER {CUTOFF_DATE}.")

    df = clean_text(df)
    df.to_csv(backup_file, index=False, encoding="utf-8-sig")
    print(f"💾 Backup saved: {backup_file}")

    if DRY_RUN:
        print("🧪 DRY RUN → No rows deleted. Review the CSV above.")
        return

    delete_query = f"""
        WITH boundary AS (
            SELECT (DATE '{CUTOFF_DATE}' + INTERVAL '1 day') AS dt_utc_start
        )
        DELETE FROM {table}
        USING boundary
        WHERE {submitted_at} IS NOT NULL
          AND CAST({submitted_at} AS text) NOT IN ('NaN', '', 'null')
          AND ({submitted_at}::timestamptz AT TIME ZONE 'UTC') >= boundary.dt_utc_start;
    """

    with engine.begin() as conn:
        conn.execute(text(delete_query))

    print(f"🗑️ Deleted {len(df)} records from {table} (strictly AFTER {CUTOFF_DATE}).")

# ------------ run ------------
for t in tables:
    process_table(t)

print("\n🎉 Done.")
print("   Mode:", "DRY RUN (no deletion)" if DRY_RUN else "LIVE DELETE")
