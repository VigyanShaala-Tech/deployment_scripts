import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
def load_env(file_path):
    load_dotenv(file_path)
    return {
        'HOST': os.getenv("DB_HOST"),
        'DB_NAME': os.getenv("DB_NAME"),
        'USER': os.getenv("DB_USER"),
        'PASSWORD': os.getenv("DB_PASSWORD"),
        'PORT': os.getenv("DB_PORT")
    }

def loading_engine(config):
    return create_engine(
        f"postgresql+psycopg2://{config['USER']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}/{config['DB_NAME']}"
    )

def insert_multiple_columns():
    config = load_env("config.env")
    engine = loading_engine(config)

    table_name = input("Enter the table name: ").strip()
    id_col = input("Enter the ID column name: ").strip()
    data_cols = [col.strip() for col in input("Enter data column names (comma-separated): ").split(",") if col.strip()]

    if not table_name or not id_col or not data_cols:
        print("Table name, ID column, and data columns are required. Exiting.")
        return

    try:
        with engine.begin() as conn:
            # Fetch current max ID
            max_id_result = conn.execute(text(f"SELECT COALESCE(MAX({id_col}), 0) FROM {table_name}"))
            max_id = max_id_result.scalar() or 0

            # Ask for number of rows
            row_count = int(input("How many rows do you want to insert? ").strip())

            data_to_insert = []
            for i in range(row_count):
                print(f"\nRow {i+1}:")
                row_values = {}
                for col in data_cols:
                    row_values[col] = input(f"Enter value for {col}: ").strip()
                data_to_insert.append((max_id + i + 1, row_values))

            # Build dynamic insert query
            col_names = ", ".join([id_col] + data_cols)
            placeholders = ", ".join([f":{id_col}"] + [f":{col}" for col in data_cols])
            insert_query = text(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})")

            # Prepare data for executemany
            params = []
            for id_val, row_values in data_to_insert:
                entry = {id_col: id_val}
                entry.update(row_values)
                params.append(entry)

            print(params)
            yes_no = input("Want to continue (yes/no): ").strip()
            if yes_no == "yes":
                conn.execute(insert_query, params)
            else:
                print("Exiting")
                return

            print(f"\n{row_count} records inserted successfully into {table_name}.")
            
    except Exception as e:
        print("Transaction failed, no records were inserted.")
        print("Error details:", e)

if __name__ == "__main__":
    insert_multiple_columns()
