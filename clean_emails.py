import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config.env")

# Database credentials
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME")

# Connect to PostgreSQL
engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}",
    echo=False
)

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
