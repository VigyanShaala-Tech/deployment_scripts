import os
from sqlalchemy import create_engine, text
from deployment_scripts.connection import get_engine, get_session, metadata

# Connect to the PostgreSQL database
engine = get_engine()

# Table and column details
table = "intermediate.student_registration_details_2"
jsonb_column = "form_details"

# Keys to rename in JSON
keys_to_rename = {
    "New_College_Name": "new_college_name",
    "Currently_Pursuing_Year": "currently_pursuing_year"
}

with engine.begin() as conn:
    for old_key, new_key in keys_to_rename.items():
        print(f"🔁 Renaming '{old_key}' to '{new_key}'...")

        sql = text(f"""
            UPDATE {table}
            SET {jsonb_column} =
                {jsonb_column} - :old_key || jsonb_build_object(:new_key, {jsonb_column}->:old_key)
            WHERE {jsonb_column} ? :old_key
        """)

        result = conn.execute(sql, {"old_key": old_key, "new_key": new_key})
        print(f"✅ Updated {result.rowcount} rows where '{old_key}' existed.")

print("Done updating JSON keys.")
