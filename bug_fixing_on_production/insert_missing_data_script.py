import os
import csv
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
import psycopg2
from psycopg2.extras import Json
from datetime import date

# --- Load environment variables ---
load_dotenv(r"config.env")

username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT", 5432)
database_name = os.getenv("DB_NAME")

CSV_FILE = r"C:\csv_file_path.csv" #specify csv file path

# --- Create SQLAlchemy engine ---
connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(connection_string)

# Read CSV and Prepare Data
gis_rows = []
student_details_data = []

with open(CSV_FILE, newline='', encoding='latin1') as f:
    reader = csv.DictReader(f)
    for row in reader:
        gis_rows.append(row)
        student_details_data.append(
            (
                row.get('Email'),
                row.get('First_name'),
                row.get('Last_name'),
                row.get('Gender') or 'F',  # fallback to 'F'
                row.get('Phone'),
                row.get('Date_of_Birth') or None,
                row.get('Caste_Category') or None,
                None,  # annual_income
                None   # location_id
            )
        )

# Bulk Insert records
conn = engine.raw_connection()
try:
    cur = conn.cursor()

    # Insert into student_details
    insert_student_query = """
        INSERT INTO intermediate.student_details (
            email, first_name, last_name, gender, phone, date_of_birth,
            caste, annual_family_income_inr, location_id
        )
        VALUES %s
        RETURNING id;
    """
    execute_values(cur, insert_student_query, student_details_data)
    student_ids = [r[0] for r in cur.fetchall()]

    # Insert into student_registration_details
    registration_data = []
    for sid, row in zip(student_ids, gis_rows):
        assigned_through = row.get("Assigned_Through")
        form_details = {
            "motivation": None,
            "new_college_name": row.get("college name"),
            "new_university_name": None,
            "reason_for_applying": None,
            "partner_organization": None,
            "currently_pursuing_year": row.get("Currently_Pursuing_Year"),
            "is_studying_STEM_fields": None,
            "how_did_you_hear_about_us": row.get("How_did_you_hear_about_us"),
            "problems_faced_in_studies_and_career": None
        }
        registration_data.append(
            (sid, assigned_through, date.today(), Json(form_details))
        )

    insert_registration_query = """
        INSERT INTO intermediate.student_registration_details (
            student_id, assigned_through, registration_date, form_details
        )
        VALUES %s;
    """
    execute_values(cur, insert_registration_query, registration_data)

    # Insert into student_education
    education_data = []
    for sid, row in zip(student_ids, gis_rows):
        education_data.append(
            (
                sid,
                row.get("Education_course_id") or None,
                None,  # subject_id
                None,  # interest_subject_id
                row.get("College_id") or None,
                row.get("University_id") or None,
                row.get("college_location_id") or None,
                None,  # start_year
                None   # end_year
            )
        )

    insert_education_query = """
        INSERT INTO intermediate.student_education (
            student_id, education_course_id, subject_id, interest_subject_id,
            college_id, university_id, college_location_id, start_year, end_year
        )
        VALUES %s;
    """
    execute_values(cur, insert_education_query, education_data)

    # Commit transaction
    conn.commit()
finally:
    cur.close()
    conn.close()

print("✅ All data inserted successfully!")