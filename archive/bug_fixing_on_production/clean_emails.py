import os
from sqlalchemy import text
from deployment_scripts.connection import get_engine, get_session, metadata

# Connect to PostgreSQL
engine = get_engine()

def clean_emails():
    try:
        with engine.begin() as conn:
            # Update raw.general_information_sheet (column is "Email")
            conn.execute(text("""
                UPDATE "raw".general_information_sheet
                SET "Email" = LOWER(TRIM("Email"))
                WHERE "Email" IS NOT NULL;
            """))
            print("✅ Cleaned emails in raw.general_information_sheet")

            # Update intermediate.student_details (column is "email")
            conn.execute(text("""
                UPDATE intermediate.student_details
                SET email = LOWER(TRIM(email))
                WHERE email IS NOT NULL;
            """))
            print("✅ Cleaned emails in intermediate.student_details")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("❌ Error during email cleaning:", e)

# Run the cleanup
clean_emails()
