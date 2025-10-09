
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config.env")

# DB credentials
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT", "5432")
database_name = os.getenv("DB_NAME")

# PostgreSQL connection string
connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(connection_string)

# Change CSV file address
csv_file_path = r"C:\User\file_name.csv"  # change to your CSV file
table_name = "intermediate.subject_mapping"

# Read CSV
df = pd.read_csv(csv_file_path)

# Separate records with and without IDs
update_df = df[df['id'].notnull()].copy()   #  Replace 'id' with the primary key column name of your table
insert_df = df[df['id'].isnull()].copy()    #  Replace 'id' with the primary key column name of your table


# UPSERT for rows with IDs
if not update_df.empty:
    records = update_df.to_dict(orient="records")
    columns = update_df.columns.tolist()

    columns_str = ", ".join(columns)
    placeholders_str = ", ".join([f":{col}" for col in columns])
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != "id"])  # Replace 'id' with the primary key column name of your table

    upsert_query = text(f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders_str})
        ON CONFLICT (id)                     --Replace 'id' with the primary key column name of your table
        DO UPDATE SET {update_clause}
    """)

    with engine.begin() as conn:
        conn.execute(upsert_query, records)

    print(f" Upserted {len(records)} records (with IDs) into '{table_name}'.")

# INSERT for rows without IDs (let DB auto-generate ID) 
if not insert_df.empty:
    insert_df = insert_df.drop(columns=['id'])    #  Replace 'id' with the primary key column name of your table
    records = insert_df.to_dict(orient="records")

    if records:
        columns = insert_df.columns.tolist()
        columns_str = ", ".join(columns)
        placeholders_str = ", ".join([f":{col}" for col in columns])

        insert_query = text(f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders_str})
        """)

        with engine.begin() as conn:
            conn.execute(insert_query, records)

        print(f"* Inserted {len(records)} new records (auto-generated IDs) into '{table_name}'.")

