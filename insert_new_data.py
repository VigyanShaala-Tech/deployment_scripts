import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables

load_dotenv('config.env')

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = "raw.general_information_schema"  # Change this to your target table


# Database connection
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

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
