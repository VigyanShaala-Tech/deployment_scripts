import pandas as pd
from sqlalchemy import text
from datetime import datetime
from deployment_scripts.connection import get_engine

# ---------------------------------------------------------
# CONFIG (LOCKED TO YOUR ACTUAL TABLE & COLUMNS)
# ---------------------------------------------------------
ENGINE = get_engine()

SCHEMA_NAME = "raw"
TABLE_NAME = "student_assignment"
FULL_TABLE = f"{SCHEMA_NAME}.{TABLE_NAME}"

STUDENT_ID_COLUMN = "student_id"
COHORT_COLUMN = "cohort_code"
TIMESTAMP_COLUMN = "submitted_at"

COHORT_VALUES = ("INC007", "INC008", "INC009")

RUN_TS = datetime.now().strftime('%Y%m%d_%H%M%S')

SUMMARY_LOG_FILE = f"deletion_summary_{RUN_TS}.log"
ROW_BACKUP_EXCEL = f"deleted_rows_backup_{RUN_TS}.xlsx"

# ---------------------------------------------------------
# FETCH ROWS TO BE DELETED
# ---------------------------------------------------------
select_query = f"""
SELECT *
FROM {FULL_TABLE}
WHERE {COHORT_COLUMN} IN ('INC007','INC008','INC009')
AND (
        {TIMESTAMP_COLUMN} IS NULL
        OR TRIM(CAST({TIMESTAMP_COLUMN} AS TEXT)) = ''
    );
"""

df = pd.read_sql(select_query, ENGINE)

if df.empty:
    print("No records found for deletion. Exiting safely.")
    exit()

# ---------------------------------------------------------
# BACKUP FULL ROWS TO EXCEL (BEFORE DELETION)
# ---------------------------------------------------------
with pd.ExcelWriter(ROW_BACKUP_EXCEL, engine="openpyxl") as writer:
    df.to_excel(writer, index=False, sheet_name="Deleted_Rows")

# ---------------------------------------------------------
# SUMMARY LOG (BEFORE DELETION)
# ---------------------------------------------------------
unique_students = df[STUDENT_ID_COLUMN].unique().tolist()
row_count = len(df)

with open(SUMMARY_LOG_FILE, "w") as log:
    log.write("=== DELETION PREVIEW SUMMARY ===\n")
    log.write(f"Timestamp        : {datetime.now()}\n")
    log.write(f"Table            : {FULL_TABLE}\n")
    log.write(f"Cohorts          : INC007, INC008, INC009\n")
    log.write(f"Total Rows Found : {row_count}\n")
    log.write(f"Excel Backup     : {ROW_BACKUP_EXCEL}\n\n")
    log.write("Unique Student IDs:\n")
    for sid in unique_students:
        log.write(f"{sid}\n")

# ---------------------------------------------------------
# CONSOLE PREVIEW
# ---------------------------------------------------------
print("\n==============================================")
print(f"Rows to be deleted  : {row_count}")
print(f"Unique students    : {len(unique_students)}")
print(f"Summary log file   : {SUMMARY_LOG_FILE}")
print(f"Excel row backup   : {ROW_BACKUP_EXCEL}")
print("==============================================\n")

# ---------------------------------------------------------
# USER CONFIRMATION
# ---------------------------------------------------------
confirm = input("Type YES to proceed with deletion or NO to cancel: ").strip().upper()

if confirm != "YES":
    print("Deletion cancelled by user.")
    exit()

# ---------------------------------------------------------
# DELETE QUERY
# ---------------------------------------------------------
delete_query = f"""
DELETE FROM {FULL_TABLE}
WHERE {COHORT_COLUMN} IN ('INC007','INC008','INC009')
AND (
        {TIMESTAMP_COLUMN} IS NULL
        OR TRIM(CAST({TIMESTAMP_COLUMN} AS TEXT)) = ''
    );
"""

try:
    with ENGINE.begin() as conn:
        result = conn.execute(text(delete_query))

    print(f"Deletion successful. Rows deleted: {result.rowcount}")

    with open(SUMMARY_LOG_FILE, "a") as log:
        log.write("\n--- DELETION EXECUTED ---\n")
        log.write(f"Rows Deleted: {result.rowcount}\n")

except Exception as e:
    print("ERROR during deletion:", str(e))

    with open(SUMMARY_LOG_FILE, "a") as log:
        log.write("\n--- DELETION FAILED ---\n")
        log.write(str(e))
