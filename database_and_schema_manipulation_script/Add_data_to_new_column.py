import os
import sys
from sqlalchemy import text


from deployment_scripts.connection import get_engine, get_session, metadata

# Connect to database
engine = get_engine()

# Inputs
table = input("Enter table name (e.g., schema.table): ").strip()
column = input("Enter new column name: ").strip()
col_type = input("Enter column type (e.g., VARCHAR(100)): ").strip()
id_column = input("Enter ID column name (e.g., id): ").strip()

# Sample data to update (replace or extend this dictionary)
data = {
    1: "4",
    2: "3",
    3: "5",
    4: "5",
    5: "3",
    6: "5",
    7: "2",
    8: "2",
    9: "2",
    10: "2",
    11: "5",
    12: "4",
    13: "2",
    14: "2",
    15: "2",
    16: "2",
    17: "1",
    18: "1",
    19: "1",
    20: "1",
    21: "1",
    22: "1",
    23: "1",
    24: "1",
    25: "1",
    26: "1",
    27: "1",
    28: "1",
    29: "1"
}

with engine.begin() as conn:
    # Add column
    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type};"))
    print(f"✅ Column '{column}' added to '{table}'.")

    # Update rows
    for row_id, value in data.items():
        conn.execute(
            text(f"UPDATE {table} SET {column} = :value WHERE {id_column} = :id"),
            {"value": value, "id": row_id}
        )
        print(f"🔁 Updated {id_column}={row_id} with {column}='{value}'")

print("✅ Done.")