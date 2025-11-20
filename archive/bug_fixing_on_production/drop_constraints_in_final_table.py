import pandas as pd
from sqlalchemy import text
from deployment_scripts.connection import get_engine
import sys

engine = get_engine()

# ---------------------------------------------------------
# HARD-CODED CONFIGURATION
# ---------------------------------------------------------
SCHEMA_NAME = "final"
TABLE_NAME = "resubmission_count_overview"
FULL_TABLE = f"{SCHEMA_NAME}.{TABLE_NAME}"

# Columns you want to enforce UNIQUE constraint on:
UNIQUE_COLUMNS = ["student_id", "resource_id"]


# ---------------------------------------------------------
# STEP 1: GET EXISTING CONSTRAINTS
# ---------------------------------------------------------
def get_existing_constraints():
    query = f"""
    SELECT con.conname, con.contype
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = '{SCHEMA_NAME}'
      AND rel.relname = '{TABLE_NAME}'
      AND con.contype IN ('u', 'p');   -- u = UNIQUE, p = PRIMARY KEY
    """
    with engine.begin() as conn:
        return pd.read_sql(query, conn)


# ---------------------------------------------------------
# STEP 2: DROP CONSTRAINT
# ---------------------------------------------------------
def drop_constraint(constraint_name):
    choice = input(f"Do you want to drop constraint '{constraint_name}'? (yes/no): ").strip().lower()

    if choice not in ("yes", "y"):
        print(f"Skipping drop: {constraint_name}")
        sys.exit("Execution stopped by user.")

    query = f"""
    ALTER TABLE {FULL_TABLE}
    DROP CONSTRAINT IF EXISTS {constraint_name} CASCADE;
    """

    with engine.begin() as conn:
        conn.execute(text(query))
        print(f"Dropped constraint: {constraint_name}")


# ---------------------------------------------------------
# STEP 3: ADD NEW UNIQUE CONSTRAINT
# ---------------------------------------------------------
def add_new_unique_constraint():
    constraint_name = f"uq_{TABLE_NAME}_{'_'.join(UNIQUE_COLUMNS)}"

    query = f"""
    ALTER TABLE {FULL_TABLE}
    ADD CONSTRAINT {constraint_name}
    UNIQUE ({', '.join(UNIQUE_COLUMNS)});
    """

    with engine.begin() as conn:
        conn.execute(text(query))
        print(f"Created new constraint: {constraint_name}")


# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
def rebuild_constraints():
    print(" Checking existing constraints...")
    existing = get_existing_constraints()

    if not existing.empty:
        print(f"Found {len(existing)} constraints. Dropping...")
        for _, row in existing.iterrows():
            drop_constraint(row["conname"])
    else:
        print("No existing constraints found.")

    print("Adding new UNIQUE constraint...")
    add_new_unique_constraint()

    print("Done. Constraint rebuild complete.")


# Run the process
rebuild_constraints()
