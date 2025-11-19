import os
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from deployment_scripts.connection import get_engine
import re

# -------------------------
# CONFIG
# -------------------------
MIGRATION_FILE = r"\archive\bug_fixing_on_production\migration.sql"

LOG_DIR = "migration_logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_PATH = os.path.join(LOG_DIR, LOG_FILE)

engine = get_engine()


def load_sql_file(path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    return [q.strip() for q in sql.split(";") if q.strip()]


def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as lf:
        lf.write(msg + "\n")


def convert_delete_to_select(sql: str) -> str:
    sql = sql.strip()

    if re.search(r"(?i)\bwhere\b", sql):
        return re.sub(r"(?i)^delete", "SELECT *", sql, count=1)

    # DELETE without WHERE
    table = re.split(r"(?i)from", sql, maxsplit=1)[1].strip().split()[0]
    return f"SELECT * FROM {table}"

def run_migration():
    log(f"----- Migration Started: {datetime.now()} -----\n")

    queries = load_sql_file(MIGRATION_FILE)

    for idx, query in enumerate(queries, start=1):
        q_lower = query.lower()

        log(f"\n=== Query #{idx} ===")
        log(query)

        # -------------------------
        # Special DELETE handling
        # -------------------------
        if q_lower.startswith("delete"):
            log("DELETE detected — previewing rows.")

            # Generate SELECT version
            select_sql = convert_delete_to_select(query)

            try:
                rows = pd.read_sql(select_sql, engine)
                count = len(rows)

                log(f"Rows matching DELETE condition: {count}")
                if count > 0:
                    log(rows.to_string(index=False))

                print(f"\n DELETE detected for Query #{idx}")
                print(f"Total rows to be deleted: {count}")
                confirm = input("Proceed with DELETE? (Y/N): ").strip().lower()

                if confirm != "y":
                    log("User cancelled DELETE\n")
                    print(" Delete cancelled.")
                    continue

                # Execute DELETE after confirmation
                with engine.begin() as conn:
                    conn.execute(text(query))

                log("DELETE executed successfully.\n")
                print(f" Deleted {count} rows.\n")

            except Exception as e:
                log(f"ERROR previewing delete: {e}")
                print(f"Error previewing delete query #{idx}. Check log.")
            continue

        # -------------------------
        # Normal query (INSERT/ALTER/UPDATE/etc.)
        # -------------------------
        try:
            with engine.begin() as conn:
                result = conn.execute(text(query))

            # Log row count where available
            try:
                count = result.rowcount
                log(f"Rows affected: {count}")
            except:
                pass

            log("Status: SUCCESS")

        except Exception as e:
            log(f"Status: FAILED")
            log(f"Error: {str(e)}")
            print(f" Error in Query #{idx}. Check logs for details.")

    log(f"\n----- Migration Completed: {datetime.now()} -----")
    print(f"\nMigration finished. Log saved to: {LOG_FILE}")


if __name__ == "__main__":
    run_migration()
