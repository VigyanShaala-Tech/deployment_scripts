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
"test@gmail.com"
"muskangupta5692339@gmail.com"
"sreejith.sreenivasan@vigyanshaala.com"
"tes55@gmail.com"
"SPj@gmail.com"
"Sreejith2288@gmail.com"
"THisisTrial@gmail.com"
"test123@gmail.com"
"Sreejith2287@gmail.com"
"testtest@gmail.com"
"muskangupta5692339mg@gmail.com"
"test12355@gmail.com"
"testdfdfdfd@gmail.com"
"testtest123@gmail.com"
"tests@gmail.com"
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
