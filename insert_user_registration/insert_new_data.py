import pandas as pd
from sqlalchemy import text
import os
import sys

# Load environment variables

connection_dir = r"C:\Users\vigya\OneDrive - VigyanShaala\Desktop\add_connection_file\deployment_scripts"
if connection_dir not in sys.path:
    sys.path.append(connection_dir)

from connection import get_engine

TABLE_NAME = "raw.general_information_sheet"  # Change this to your target table


# Database connection
engine = get_engine()

# CSV to PostgreSQL in chunks

csv_file = r"C:\Users\vigya\Downloads\registration_sheet\google_form_records_sheet.csv"  # Change to your CSV path
df = pd.read_csv(csv_file, encoding='iso-8859-1')

df.to_sql(
    name=TABLE_NAME.split('.')[-1],  # Table name without schema
    con=engine,
    schema=TABLE_NAME.split('.')[0] if '.' in TABLE_NAME else None,
    if_exists="append",  # Append without overwriting
    index=False,
    method="multi",  # Enables bulk insert
    chunksize=1000   # Adjust chunk size for your system
)

print(f"Successfully inserted {len(df)} records into {TABLE_NAME} in chunks.")