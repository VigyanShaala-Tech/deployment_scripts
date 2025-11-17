from datetime import datetime
import pandas as pd
import os
from sqlalchemy import text
from deployment_scripts.connection import get_engine, get_session, metadata


# --- CONFIG ---
MIGRATION_FILE = r"archive\bug_fixing_on_production\migration.sql"
LOG_FILE = f"deleted_rows_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

engine = get_engine()

# --- Read entire SQL file ---
with open(MIGRATION_FILE, "r", encoding="utf-8") as f:
    sql = f.read().strip()

# Split the file into separate SQL statements if needed
# (Assumes each statement ends with a semicolon)
queries = [q.strip() for q in sql.split(";") if q.strip()]

# Expect file to have SELECT first and DELETE second
select_query = next((q for q in queries if q.lower().startswith("select")), None)
delete_query = next((q for q in queries if q.lower().startswith("delete")), None)
#select_query, delete_query = sql.split(";", 1)

# --- Run SELECT query to preview deletions ---
rows = pd.read_sql(select_query, engine)

if rows.empty:
    print(" No rows found to delete.")
else:
    # Log what will be deleted
    with open(LOG_FILE, "w", encoding="utf-8") as log:
        log.write(f"Deletion Log - {datetime.now()}\n")
        log.write(f"Total rows: {len(rows)}\n\n")
        log.write(rows.to_string(index=False))
    print(f" Log saved as {LOG_FILE}")
    print(f" Total rows identified for deletion: {len(rows)}")

    # --- Ask for confirmation before deleting ---
    confirm = input(f" Proceed to delete {len(rows)} rows? (Y/N): ").strip().lower()

    if confirm == "y":
        with engine.begin() as conn:
            conn.execute(text(delete_query))
        print(f" Deleted {len(rows)} rows successfully.")
    else:
        print(" Deletion cancelled by user. No changes made.")