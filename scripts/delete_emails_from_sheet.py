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

# Table details
schema_name = "raw"
table_name = "general_information_sheet"
email_column = "Email"  # Replace with the exact column name in your table

# List of emails to delete
emails_to_delete = [
"Sreejith2288@gmail.com",
"SPj@gmail.com",
"tes55@gmail.com",
"sreejith.sreenivasan@vigyanshaala.com",
"muskangupta5692339@gmail.com",
"test@gmail.com"
]  #Will add all email ids that are present in our general_info_sheet later above are test emails

# Delete rows
with engine.connect() as conn:
    for email in emails_to_delete:
        result = conn.execute(
            text(f'''
                DELETE FROM {schema_name}.{table_name}
                WHERE "{email_column}" = :email
            '''), {"email": email}
        )
        print(f"Deleted rows with email: {email}")

    conn.commit()

print("✅ All specified emails have been deleted from the table.")
