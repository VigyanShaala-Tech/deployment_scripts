import os
import sys
import pandas as pd
from sqlalchemy import text
import textwrap

from deployment_scripts.connection import get_engine, get_session, metadata

# SQLAlchemy engine
engine = get_engine()

# -------------------------------
# Function to handle duplicates, CSV export, and unique constraint
# -------------------------------
def prepare_table_for_upsert(table_name, unique_columns, csv_filename):
    # Step 1: Detect duplicates
    dup_query = f"""
    SELECT {', '.join(unique_columns)}, COUNT(*) AS duplicate_count
    FROM {table_name}
    GROUP BY {', '.join(unique_columns)}
    HAVING COUNT(*) > 1;
    """
    with engine.begin() as conn:
        dup_df = pd.read_sql(dup_query, conn)
        if not dup_df.empty:
            dup_df.to_csv(csv_filename, index=False)
            print(f"* Duplicates exported to {csv_filename}")
        else:
            print(f"* No duplicates found in {table_name}.")

    # Step 2: Remove duplicates (keep first occurrence)
    delete_query = f"""
    DELETE FROM {table_name} a
    USING {table_name} b
    WHERE a.ctid < b.ctid
    AND {" AND ".join([f"a.{col} = b.{col}" for col in unique_columns])};
    """
    with engine.begin() as conn:
        conn.execute(text(delete_query))
        print(f"* Duplicates cleaned from {table_name}.")


    # Step 3: Add unique constraint
    constraint_name = f"uq_{table_name.replace('.', '_')}"
    add_constraint_query = f"""
    ALTER TABLE {table_name}
    ADD CONSTRAINT {constraint_name} UNIQUE ({', '.join(unique_columns)});
    """
    with engine.begin() as conn:
        try:
            conn.execute(text(add_constraint_query))
            print(f"* Unique constraint added on {table_name} ({', '.join(unique_columns)}).")
        except Exception as e:
            print(f"* Unique constraint might already exist for {table_name}: {e}")


def clean_general_information_sheet():
    dup_query = """
    SELECT "Email", COUNT(*) AS duplicate_count
    FROM old.general_information_sheet
    GROUP BY "Email"
    HAVING COUNT(*) > 1;
    """
    with engine.begin() as conn:
        dup_df = pd.read_sql(dup_query, conn)
        if not dup_df.empty:
            dup_df.to_csv("duplicate_general_information_sheet.csv", index=False)
            print("* Duplicates exported to duplicate_general_information_sheet.csv")
            
            delete_query = """
            DELETE FROM old.general_information_sheet a
            USING old.general_information_sheet b
            WHERE a.ctid < b.ctid
              AND a."Email" = b."Email";
            """
            conn.execute(text(delete_query))
            print("* Duplicates removed from raw.general_information_sheet.")
        else:
            print("* No duplicates found in raw.general_information_sheet.")

# -------------------------------
# Prepare tables for upsert
# -------------------------------
clean_general_information_sheet()
#prepare_table_for_upsert("final.final_quiz", ["student_id", "resource_id"], "duplicate_final_quiz.csv")
#prepare_table_for_upsert("final.final_assignment", ["student_id", "resource_id", "submitted_at"], "duplicate_final_assignment.csv")
#prepare_table_for_upsert("final.daily_weekly_attendance",["student_id", "session_id"],"duplicate_daily_weekly_student_attendance.csv")

#------------------------------------
#Student demography query
#-----------------------------------
resubmission_count_overview_upsert_query = textwrap.dedent("""
WITH submission_counts AS (
  SELECT
    student_id,
    resource_id,
    title,
    cohort_code,
    college_name,
    COUNT(*) FILTER (WHERE submission_status = 'under review') AS under_review_count,
    COUNT(*) FILTER (WHERE submission_status = 'reviewed') AS accepted_count,
    COUNT(*) FILTER (WHERE submission_status = 'rejected') AS rejected_count,
    MAX(submitted_at) FILTER (WHERE submission_status = 'under review') AS last_submission_date
  FROM intermediate.final_assignment
  GROUP BY student_id, resource_id, title, cohort_code, college_name
)
INSERT INTO final.resubmission_count_overview (
    student_id,
    email_id,
    resource_id,
    title,
    cohort_code,
    college_name,
    total_submissions,
    resubmissions_count,
    resubmission_rate,
    accepted_count,
    acceptance_rate,
    rejected_count,
    rejection_rate,                                                                                                             
    last_submission_date
)
SELECT
  sc.student_id,
  sd.email AS email_id,
  sc.resource_id,
  sc.title,
  sc.cohort_code,
  sc.college_name,
  sc.under_review_count AS total_submissions,
  CASE WHEN sc.under_review_count > 1 THEN sc.under_review_count - 1 ELSE 0 END AS resubmissions_count,
  CASE 
    WHEN sc.under_review_count > 1 
    THEN ROUND(( (sc.under_review_count - 1)::numeric / sc.under_review_count ) * 100, 2)
    ELSE 0 
  END AS resubmission_rate,
  sc.accepted_count,
  ROUND((sc.accepted_count::numeric / NULLIF(sc.under_review_count, 0)) * 100, 2) AS acceptance_rate,
  sc.rejected_count,
  ROUND((sc.rejected_count::numeric / NULLIF(sc.under_review_count, 0)) * 100, 2) AS rejection_rate,
  sc.last_submission_date
FROM submission_counts sc
LEFT JOIN raw.student_details sd
  ON sc.student_id = sd.id
ON CONFLICT (student_id, resource_id, last_submission_date)
DO UPDATE SET
    email_id = EXCLUDED.email_id,
    title = EXCLUDED.title,
    cohort_code = EXCLUDED.cohort_code,
    college_name = EXCLUDED.college_name,
    total_submissions = EXCLUDED.total_submissions,
    resubmissions_count = EXCLUDED.resubmissions_count,
    resubmission_rate = EXCLUDED.resubmission_rate,                                                                                                              
    accepted_count = EXCLUDED.accepted_count,
    acceptance_rate = EXCLUDED.acceptance_rate,                                                       
    rejected_count = EXCLUDED.rejected_count,
    rejection_rate = EXCLUDED.rejection_rate,                                                                                                              
    last_submission_date = EXCLUDED.last_submission_date;                                                                                                                                                                 
""")

student_registration_overview_upsert_query = textwrap.dedent("""
WITH student_demography AS (
    SELECT 
        student_id,
        email,
        caste,
        annual_family_income_inr,
        "Incubator_Batch",
        state_union_territory,
        district,
        country,
        city_category,
        form_details,
        education_category,
        subject_areas,
        sub_fields_list,
        course_name,
        college_name,
        university_name
    FROM intermediate.student_demography
),
student_registration_details AS (
    SELECT 
        srd.id,
        srd.student_id,
        srd.assigned_through,
        srd.registration_date::TIMESTAMP AS registration_date
    FROM raw.student_registration_details srd
),
student_details AS (
    SELECT 
        sd.id,
        sd.email,
        sd.phone
    FROM raw.student_details sd
)
INSERT INTO final.student_registration_overview (
    student_id,
    email,
    phone,
    caste,
    annual_family_income_inr,
    "Incubator_Batch",
    state_union_territory,
    district,
    country,
    city_category,
    form_details,
    education_category,
    subject_areas,
    sub_fields_list,
    course_name,
    college_name,
    university_name,
    registration_date
)
SELECT
    sdm.student_id,
    sdm.email,
    sd.phone,
    sdm.caste,
    sdm.annual_family_income_inr,
    sdm."Incubator_Batch",
    sdm.state_union_territory,
    sdm.district,
    sdm.country,
    sdm.city_category,
    sdm.form_details,
    sdm.education_category,
    sdm.subject_areas,
    sdm.sub_fields_list,
    sdm.course_name,
    sdm.college_name,
    sdm.university_name,
    srd.registration_date
FROM student_demography sdm
INNER JOIN student_details sd ON sdm.student_id = sd.id
INNER JOIN student_registration_details srd ON sd.id = srd.student_id
ON CONFLICT (student_id, registration_date)
DO UPDATE SET
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    caste = EXCLUDED.caste,
    annual_family_income_inr = EXCLUDED.annual_family_income_inr,
    "Incubator_Batch" = EXCLUDED."Incubator_Batch",
    state_union_territory = EXCLUDED.state_union_territory,
    district = EXCLUDED.district,
    country = EXCLUDED.country,
    city_category = EXCLUDED.city_category,
    form_details = EXCLUDED.form_details,
    education_category = EXCLUDED.education_category,
    subject_areas = EXCLUDED.subject_areas,
    sub_fields_list = EXCLUDED.sub_fields_list,
    course_name = EXCLUDED.course_name,
    college_name = EXCLUDED.college_name,
    university_name = EXCLUDED.university_name,
    registration_date = EXCLUDED.registration_date;
""")

with engine.begin() as conn:
    conn.execute(text(resubmission_count_overview_upsert_query))
    print("* Data upserted to 'final.resubmission_count_overview'.")

    conn.execute(text(student_registration_overview_upsert_query))
    print("* Data upserted to 'final.student_registration_overview'.")