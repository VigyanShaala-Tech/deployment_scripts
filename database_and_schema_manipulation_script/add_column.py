import os
import sys
from sqlalchemy import text

from deployment_scripts.connection import get_engine, get_session, metadata

# Get user input
full_table_name = input("Enter full table name (e.g., schema_name.table_name): ").strip()
new_column_name = input("Enter name of the new column: ").strip()
new_column_type = input("Enter data type of the new column (e.g., VARCHAR(255), INTEGER): ").strip()


# SQL query
add_column_query = text(f"""
    ALTER TABLE {full_table_name}
    ADD COLUMN {new_column_name} {new_column_type}
""")

# PostgreSQL connection
engine = get_engine()

# Execute query
with engine.connect() as conn:
    conn.execute(add_column_query)
    conn.commit()

print(f"Successfully added column '{new_column_name}' of type '{new_column_type}' to table '{full_table_name}'.")