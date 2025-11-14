import os
import textwrap
from sqlalchemy import create_engine, text
from deployment_scripts.connection import get_engine, get_session, metadata

#Shema
TARGET_SCHEMA = "final"

# Set to True to drop existing tables before creating
DROP_IF_EXISTS = False

engine = get_engine()

# Helper to prefix CREATE TABLE with schema and table name
def wrap_create(schema: str, table: str, body_sql: str) -> str:
    # Ensure the body_sql does NOT already contain a CREATE TABLE line.
    header = f"CREATE TABLE IF NOT EXISTS {schema}.{table} AS\n"
    return header + body_sql.lstrip()

# ---- Body SQLs (without the CREATE TABLE ... AS line) ----
resubmission_count_overview_body = textwrap.dedent("""
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
  FROM
    "intermediate"."final_assignment"
  GROUP BY
    student_id,
    resource_id,
    title,
    course_name,
    cohort_code,
	college_name
)
SELECT
  sc.student_id,
  (
    SELECT sd.email
    FROM "raw"."student_details" sd
    WHERE sd.id = sc.student_id
    LIMIT 1
  ) AS email_id,
  sc.resource_id,
  sc.title,
  sc.cohort_code,
  sc.college_name,
  sc.under_review_count AS total_submissions,
  CASE
    WHEN sc.under_review_count > 1 THEN sc.under_review_count - 1
    ELSE 0
  END AS resubmissions_count,
  CASE 
    WHEN sc.under_review_count > 1 
      THEN ROUND(((sc.under_review_count - 1)::numeric / sc.under_review_count) * 100, 2)
    ELSE 0 
  END AS resubmission_rate,
  sc.accepted_count,
  ROUND((sc.accepted_count::numeric / NULLIF(sc.under_review_count, 0)) * 100, 2) AS acceptance_rate,
  sc.rejected_count,
  ROUND((sc.rejected_count::numeric / NULLIF(sc.under_review_count, 0)) * 100, 2) AS rejection_rate,
  sc.last_submission_date
FROM submission_counts sc
ORDER BY sc.student_id, sc.resource_id;
""")

student_registration_overview_body = textwrap.dedent("""
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
INNER JOIN student_details sd
    ON sdm.student_id = sd.id
INNER JOIN student_registration_details srd
    ON sd.id = srd.student_id;
""")

# Map desired final table names
table_map = [
    ("resubmission_count_overview", resubmission_count_overview_body),
    ("student_registration_overview", student_registration_overview_body)
]

def save_sql_file(schema: str, table: str, sql_text: str):
    base = f"{schema}_{table}"
    fname = f"./{base}.sql"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(sql_text)
    print(f"Saved SQL to {fname}")

def drop_table_if_exists(conn, schema: str, table: str):
    q = text(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;")
    conn.execute(q)
    print(f"Dropped table if existed: {schema}.{table}")

def run():
    with engine.begin() as conn:
        # ensure target schema exists (create if missing)
        try:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))
            print(f"Schema ensured: {TARGET_SCHEMA}")
        except Exception as e:
            print(f"Warning: could not create/ensure schema {TARGET_SCHEMA}: {e}")

        for table_name, body in table_map:
            full_sql = wrap_create(TARGET_SCHEMA, table_name, body)
            print(f"\n--- Preparing to create {TARGET_SCHEMA}.{table_name} ---")
            save_sql_file(TARGET_SCHEMA, table_name, full_sql)
            if DROP_IF_EXISTS:
                drop_table_if_exists(conn, TARGET_SCHEMA, table_name)
            try:
                print(f"Creating table {TARGET_SCHEMA}.{table_name} ...")
                conn.execute(text(full_sql))
                print(f"Created {TARGET_SCHEMA}.{table_name} successfully.")
            except Exception as e:
                print(f"Error creating {TARGET_SCHEMA}.{table_name}: {e}")

        conn.execute(text(f"""
            ALTER TABLE {TARGET_SCHEMA}.resubmission_count_overview
            ADD CONSTRAINT resubmission_count_overview_unique
            UNIQUE (student_id, resource_id, last_submission_date);
        """))

        conn.execute(text(f"""
            ALTER TABLE {TARGET_SCHEMA}.student_registration_overview
            ADD CONSTRAINT student_registration_overview_unique
            UNIQUE (student_id, registration_date);
        """))

        print("Unique constraints added successfully.")
    
    print("\nAll operations completed.")

if __name__ == "__main__":
    run()