import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load config.env file
load_dotenv("config.env")

# Load DB credentials from environment
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT") 
database_name = os.getenv("DB_NAME")

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
connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(connection_string)

# Execute query
with engine.connect() as conn:
    conn.execute(add_column_query)
    conn.commit()

print(f"Successfully added column '{new_column_name}' of type '{new_column_type}' to table '{full_table_name}'.")