from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# --- Load DB credentials from .env file ---
load_dotenv("config.env")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# Connect to PostgreSQL 
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Define schema.table list 
tables_to_process = [
    "intermediate.subject_mapping",
    "intermediate.course_mapping",
    "intermediate.location_mapping",
    "intermediate.college_mapping",
    "intermediate.university_mapping",
]

print("Start: auto_create_sequences")

with engine.connect() as conn:
    for full_table_name in tables_to_process:
        schema, table = full_table_name.split(".")
        print(f"\nProcessing {full_table_name}")

        # Detect primary key column 
        pk_query = text(f"""
            SELECT a.attname AS pk_column
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{schema}.{table}'::regclass
            AND i.indisprimary;
        """)
        pk_result = conn.execute(pk_query).fetchone()
        if not pk_result:
            print(f"No primary key found for {full_table_name}, skipping.")
            continue
        pk_column = pk_result.pk_column
        print(f"Detected primary key: {pk_column}")

        # Build sequence name dynamically 
        seq_name = f"{schema}.{table}_{pk_column}_seq"

        # Build and run SQL script 
        sql_script = f"""
        -- 1 Create the sequence if it doesn't exist
        CREATE SEQUENCE IF NOT EXISTS {seq_name}
            AS BIGINT
            START WITH 1
            INCREMENT BY 1
            OWNED BY {schema}.{table}.{pk_column};

        -- 2 Attach sequence as default to the PK column
        ALTER TABLE {schema}.{table}
        ALTER COLUMN {pk_column} SET DEFAULT nextval('{seq_name}'::regclass);

        -- 3 Sync sequence value to MAX(pk)+1
        SELECT setval(
            '{seq_name}',
            COALESCE((SELECT MAX({pk_column}) FROM {schema}.{table}), 0) + 1,
            false
        );
        """
        try:
            conn.execute(text(sql_script))
            conn.commit()
            print(f" Sequence created/synced for {full_table_name}.{pk_column}")
        except Exception as e:
            print(f" Error processing {full_table_name}: {e}")

print("\nFinished: auto_create_sequences")
