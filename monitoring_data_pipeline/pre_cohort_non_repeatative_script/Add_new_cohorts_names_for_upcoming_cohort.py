import os
import sys
from sqlalchemy import text

from deployment_scripts.connection import get_engine, get_session, metadata

def insert_multiple_columns():
    engine = get_engine()

    table_name = input("Enter the table name: ").strip()
    id_col = input("Enter the ID column name: ").strip()
    data_cols = [col.strip() for col in input("Enter data column names (comma-separated): ").split(",") if col.strip()]

    if not table_name or not id_col or not data_cols:
        print("Table name, ID column, and data columns are required. Exiting.")
        return

    try:
        with engine.begin() as conn:
            # Ask for number of rows
            row_count = int(input("How many rows do you want to insert? ").strip())

            data_to_insert = []
            for i in range(row_count):
                print(f"\nRow {i+1}:")
                row_values = {}
                
                # Always ask for the primary key (text allowed)
                row_values[id_col] = input(f"Enter value for {id_col}: ").strip()
                
                for col in data_cols:
                    row_values[col] = input(f"Enter value for {col}: ").strip()
                
                data_to_insert.append(row_values)

            # Build dynamic insert query
            col_names = ", ".join([id_col] + data_cols)
            placeholders = ", ".join([f":{col}" for col in [id_col] + data_cols])
            insert_query = text(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})")

            print(data_to_insert)
            yes_no = input("Want to continue (yes/no): ").strip().lower()
            if yes_no == "yes":
                conn.execute(insert_query, data_to_insert)
                print(f"\n{row_count} records inserted successfully into {table_name}.")
            else:
                print("Exiting without inserting.")

    except Exception as e:
        print("Transaction failed, no records were inserted.")
        print("Error details:", e)

if __name__ == "__main__":
    insert_multiple_columns()
