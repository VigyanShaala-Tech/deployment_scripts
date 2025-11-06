import os
from sqlalchemy import text

from deployment_scripts.connection import get_engine, get_session, metadata

# Get user input from terminal
full_table_name = input("Enter full table name (e.g., schema_name.table_name): ").strip()
column_to_update = input("Enter column name to update: ").strip()
new_value = input("Enter new value to set: ").strip()
where_column = input("Enter column name for WHERE clause: ").strip()
where_value = input("Enter value to match in WHERE clause: ").strip()

# SQL query
update_query = text(f"""
    UPDATE {full_table_name}
    SET {column_to_update} = :new_value
    WHERE {where_column} = :where_value
""")

# PostgreSQL connection
engine = get_engine()

# Query Execution
with engine.connect() as conn:
    conn.execute(update_query, {
        "new_value": new_value,
        "where_value": where_value
    })
    conn.commit()

print(f"Successfully updated '{column_to_update}' to '{new_value}' where '{where_column}' = '{where_value}' in {full_table_name}.")