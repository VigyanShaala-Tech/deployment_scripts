import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config.env")

# Get DB credentials
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database_name = os.getenv("DB_NAME")

# Table name
full_table_name = "intermediate.student_details"

# DB connection
connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(connection_string)

# Step 1: Load all student details
with engine.connect() as conn:
    df = pd.read_sql(f"SELECT * FROM {full_table_name}", conn)

# Step 2: Load referenced IDs
with engine.connect() as conn:
    referenced_ids_df = pd.read_sql("SELECT DISTINCT student_id FROM intermediate.student_registration_details", conn)
referenced_ids = set(referenced_ids_df['student_id'].tolist())

# Step 3: For each email, keep only the row with the max ID
df_sorted = df.sort_values(by="id")
df_keep = df_sorted.groupby("email", as_index=False)["id"].max()  # latest ID per email
ids_to_keep = set(df_keep["id"].tolist())

# Step 4: Find duplicates to delete (not in ids_to_keep)
df_duplicates = df[~df["id"].isin(ids_to_keep)]

# Step 5: Split into unreferenced and referenced duplicates
df_unreferenced_duplicates = df_duplicates[~df_duplicates["id"].isin(referenced_ids)]
df_referenced_duplicates = df_duplicates[df_duplicates["id"].isin(referenced_ids)]

# Step 6: Delete unreferenced duplicates from DB
with engine.begin() as conn:
    for duplicate_id in df_unreferenced_duplicates["id"]:
        delete_query = text(f"DELETE FROM {full_table_name} WHERE id = :id")
        conn.execute(delete_query, {"id": int(duplicate_id)})

# Step 7: Save deleted rows
output_excel_path = "deleted_unreferenced_duplicates.xlsx"
df_unreferenced_duplicates.to_excel(output_excel_path, index=False)

# Step 8: Save skipped referenced rows
referenced_output_path = "skipped_referenced_duplicates.xlsx"
df_referenced_duplicates.to_excel(referenced_output_path, index=False)

# Final messages
print(f"Deleted {len(df_unreferenced_duplicates)} unreferenced duplicate rows.")
print(f"Deleted rows saved to: {output_excel_path}")
print(f"Skipped {len(df_referenced_duplicates)} referenced duplicate rows (not deleted).")
print(f"Skipped rows saved to: {referenced_output_path}")
