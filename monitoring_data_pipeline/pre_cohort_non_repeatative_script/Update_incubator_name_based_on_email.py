import sys
import os
from sqlalchemy import update, Table
import pandas as pd

from deployment_scripts.connection import get_engine, get_session, metadata


# Load email list from CSV
df = pd.read_csv(r"C:\Users\vigya\Downloads\Incubator_9_email_id_mappings.csv", encoding='ISO-8859-1')  
emails = df['Email'].str.strip().tolist()

# Get new values from terminal
NEW_INCUBATOR_BATCH = input("Enter new Incubator Batch: ").strip()
NEW_INCUBATOR_COURSE_NAME = input("Enter new Incubator Course Name: ").strip()

# Set up DB connection
engine = get_engine()
session = get_session()

# Reflect table
metadata.reflect(bind=engine, schema="raw")
table = Table("general_information_sheet", metadata, autoload_with=engine, schema="raw")

# Query existing emails from DB
db_emails = pd.read_sql(f"SELECT \"Email\" FROM raw.general_information_sheet", engine)['Email'].str.strip().tolist()

# Separate matched and unmatched
matched_emails = list(set(emails) & set(db_emails))
unmatched_emails = list(set(emails) - set(db_emails))

# Update matched emails
if matched_emails:
    stmt = (
        update(table)
        .where(table.c.Email.in_(matched_emails))
        .values(
            Incubator_Batch=NEW_INCUBATOR_BATCH,
            Incubator_Course_Name=NEW_INCUBATOR_COURSE_NAME
        )
    )
    session.execute(stmt)
    session.commit()
    print(f"Updated {len(matched_emails)} records.")
else:
    print("No matching emails found to update.")

# Save unmatched emails to CSV
if unmatched_emails:
    pd.DataFrame({"Unmatched_Email": unmatched_emails}).to_csv("unmatched_emails.csv", index=False)
    print(f"Saved {len(unmatched_emails)} unmatched emails to unmatched_emails.csv")
else:
    print("All emails matched. No unmatched emails found.")

session.close()
