import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load config.env file
load_dotenv("config.env")

# Load DB credentials
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database_name = os.getenv("DB_NAME")

# PostgreSQL connection
connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(connection_string)

# Schema and table
schema_name = "raw"
table_name = "general_information_sheet"
id_column = "Student_id"  # Case sensitive column name

with engine.connect() as conn:
    # Step 1: Get max existing ID (use double quotes around column name)
    max_id_result = conn.execute(
        text(f'SELECT MAX("{id_column}") FROM {schema_name}.{table_name}')
    )
    max_id = max_id_result.scalar() or 0
    next_id = max_id + 1

    # Step 2: Get rows with NULL ID (double quotes around id_column)
    null_id_rows = conn.execute(
        text(f'SELECT ctid FROM {schema_name}.{table_name} WHERE "{id_column}" IS NULL')
    ).fetchall()

    # Step 3: Update each row with a new ID
    for row in null_id_rows:
        ctid = row[0]  # ctid is a unique row identifier in PostgreSQL
        conn.execute(
            text(f'''
                UPDATE {schema_name}.{table_name}
                SET "{id_column}" = :new_id
                WHERE ctid = :ctid
            '''),
            {"new_id": next_id, "ctid": ctid}
        )
        print(f"Assigned ID {next_id} to row with CTID {ctid}")
        next_id += 1

    conn.commit()

print("✅ All missing IDs have been filled.")
