import os
from sqlalchemy import create_engine, text
from deployment_scripts.connection import get_engine, get_session, metadata

engine = get_engine()
 
# Table details
schema_name = "raw"
table_name = "general_information_sheet"
email_column = "Email"
 
# List of emails to delete (commas added!)
emails_to_delete = [
    "test@gmail.com",
    "muskangupta5692339@gmail.com",
    "sreejith.sreenivasan@vigyanshaala.com",
    "tes55@gmail.com",
    "SPj@gmail.com",
    "Sreejith2288@gmail.com",
    "THisisTrial@gmail.com",
    "test123@gmail.com",
    "Sreejith2287@gmail.com",
    "testtest@gmail.com",
    "muskangupta5692339mg@gmail.com",
    "test12355@gmail.com",
    "testdfdfdfd@gmail.com",
    "testtest123@gmail.com",
    "tests@gmail.com"
]
 
# Delete rows
with engine.begin() as conn:  # handles commit automatically
    for email in emails_to_delete:
        result = conn.execute(
            text(f'''
                DELETE FROM {schema_name}.{table_name}
                WHERE TRIM("{email_column}") = :email
            '''), {"email": email}
        )
        print(f"Deleted rows with email: {email}, Rows affected: {result.rowcount}")
 
print("✅ All specified emails have been deleted from the table.")