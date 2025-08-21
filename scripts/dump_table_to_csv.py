import psycopg2
import pandas as pd
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config.env")

# Get table name from CLI argument
if len(sys.argv) < 2:
    print("Usage: python dump_table_to_csv.py <schema.table_name>")
    sys.exit(1)

table_name = sys.argv[1]  # e.g., intermediate.student_details

# Database connection
try:
    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME")
    )


    print(f"🔄 Fetching data from `{table_name}`...")

    # Read SQL table into pandas DataFrame
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)

    # Output file name
    filename = table_name.replace('.', '_') + ".csv"
    output_path = os.path.join(os.getcwd(), filename)

    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"✅ Table dumped to CSV: {output_path}")

except Exception as e:
    print(f"Error: {e}")

finally:
    if 'conn' in locals():
        conn.close()

#Example : python dump_table_to_csv.py intermediate.student_registration_details