import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

CSV_FILE = r"C:\Users\vigya\OneDrive - VigyanShaala\02 Products  Initiatives\01 SheForSTEM\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Kalpana\11 Live_Session_Data\Live_session_data.csv"   # your CSV file path
TABLE_NAME = "intermediate.live_session"  # target table
load_dotenv("config.env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432))
}

# Function to convert Batch -> cohort_code
def convert_batch(batch_value: str) -> str:
    if not batch_value:
        return None
    parts = batch_value.split()
    if len(parts) != 2:
        return None
    program, number = parts[0], parts[1]
    if program.lower() == "incubator":
        code_prefix = "INC"
    elif program.lower() == "accelerator":
        code_prefix = "ACC"
    else:
        code_prefix = program[:3].upper()
    # Pad with zeros (7.0 -> 007, 8.0 -> 008)
    code_number = str(number).replace(".0", "").zfill(3)
    return f"{code_prefix}{code_number}"

# Function to convert date dd-mon-yy -> yyyy-mm-dd
def convert_date(date_value: str) -> str:
    try:
        return datetime.strptime(date_value, "%d-%b-%y").strftime("%Y-%m-%d")
    except Exception:
        return None

# Read CSV
df = pd.read_csv(CSV_FILE)

# Transform columns
df["cohort_code"] = df["Batch"].apply(convert_batch)
df["session_name"] = df["Topic"]
df["type"] = df["Session Type"]
df["code"] = df["Session Code"]
df["duration_in_sec"] = 3600
df["conducted_on"] = df["Date"].apply(convert_date)

# Keep only required columns
final_df = df[["cohort_code", "session_name", "type", "code", "duration_in_sec", "conducted_on"]]

# Connect to DB
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

cleanup_query = f"""
DELETE FROM {TABLE_NAME} a
USING {TABLE_NAME} b
WHERE a.ctid < b.ctid
  AND a.cohort_code = b.cohort_code
  AND a.code = b.code
  AND a.conducted_on = b.conducted_on;
"""

cur.execute(cleanup_query)
# Ensure composite unique constraint exists (ignore error if already exists)
alter_query = f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'session_unique_cohort_code'
        AND conrelid = '{TABLE_NAME}'::regclass
    ) THEN
        ALTER TABLE {TABLE_NAME}
        ADD CONSTRAINT session_unique_cohort_code UNIQUE (cohort_code, code, conducted_on);
    END IF;
END$$;
"""
cur.execute(alter_query)

# UPSERT query with composite key (cohort_code, code)
insert_query = f"""
    INSERT INTO {TABLE_NAME} 
    (cohort_code, session_name, type, code, duration_in_sec, conducted_on)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (cohort_code, code, conducted_on) DO UPDATE SET
        session_name    = EXCLUDED.session_name,
        type            = EXCLUDED.type,
        duration_in_sec = EXCLUDED.duration_in_sec,
        conducted_on    = EXCLUDED.conducted_on;
"""

# Insert / Upsert each row
for _, row in final_df.iterrows():
    cur.execute(insert_query, tuple(row))

conn.commit()
cur.close()
conn.close()

print("✅ Data successfully upserted into database using (cohort_code, code, conducted_on).")
