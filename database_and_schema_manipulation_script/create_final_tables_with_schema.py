import os
import textwrap
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load .env file
load_dotenv("config.env")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

#Schema
TARGET_SCHEMA = "final"


# Set to True to drop existing tables before creating
DROP_IF_EXISTS = False

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True,
)

# Function to CREATE TABLE with schema and table name
def wrap_create(schema: str, table: str, body_sql: str) -> str:
    # Ensure the body_sql does NOT already contain a CREATE TABLE line.
    header = f"CREATE TABLE IF NOT EXISTS {schema}.{table} AS\n"
    return header + body_sql.lstrip()

# ---- Body SQLs (without the CREATE TABLE ... AS line) ----
student_demography_body = textwrap.dedent("""
WITH student_details AS (
    SELECT
        sd.id,
        sd.email,
        sd.caste,
        sd.annual_family_income_inr,
        sd.location_id,
        gs."Incubator_Batch"
    FROM intermediate.student_details sd
    LEFT JOIN raw.general_information_sheet gs
        ON sd.id = gs."Student_id"
),
student_registration AS (
    SELECT
        student_id,
        form_details
    FROM intermediate.student_registration_details
),
mapped_subjects AS (
    SELECT 
        se.student_id,
        se.education_course_id,
        cm.course_name,
        colm.standard_college_names AS college_name,
        um.standard_university_names AS university_name,
        sm.education_category,
        sm.subject_area,
        sm.sub_field
    FROM intermediate.student_education se
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id) ON TRUE     --unnest() function will create one row for each element in the array subject_id
    JOIN intermediate.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN intermediate.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN intermediate.college_mapping colm           -- Left join as we don't have all the colleges listed in our standardized table i.e college_mapping table
        ON se.college_id = colm.college_id
    LEFT JOIN intermediate.university_mapping um
        ON se.university_id = um.university_id            -- Left join as we don't have all the universities listed in our standardized table i.e university_mapping table
),
aggregated_subjects AS (
    SELECT
        student_id,
        education_course_id,
        string_agg(DISTINCT education_category, ', ') AS education_category,
        string_agg(DISTINCT subject_area, ', ') AS subject_areas,
        string_agg(DISTINCT sub_field, ', ') AS sub_fields_list
    FROM mapped_subjects
    GROUP BY student_id, education_course_id
),
non_aggregated AS (
    SELECT DISTINCT
        student_id,
        education_course_id,
        course_name,
        college_name,
        university_name
    FROM mapped_subjects
)
SELECT
    sd.id AS student_id,
    sd.email,
    sd.caste,
    sd.annual_family_income_inr,
    sd."Incubator_Batch",
    lm.state_union_territory,
    lm.district,
    lm.country,
    lm.city_category,
    sr.form_details,
    asub.education_category,
    asub.subject_areas,
    asub.sub_fields_list,
    na.course_name,
    na.college_name,
    na.university_name
FROM student_details sd
LEFT JOIN intermediate.location_mapping lm          --INNER JOIN would have exclude INC 1,2,3 records because no match would be found in the standardized file.
    ON sd.location_id = lm.location_id
LEFT JOIN student_registration sr                   --Ensures no data from the student details table is lost, even if there is no corresponding entry in the student_registration table.
    ON sd.id = sr.student_id
LEFT JOIN aggregated_subjects asub
    ON sd.id = asub.student_id
LEFT JOIN non_aggregated na
    ON sd.id = na.student_id
    AND asub.education_course_id = na.education_course_id;
""")

student_attendance_body = textwrap.dedent("""
WITH cohort_range AS (
    SELECT start_date, end_date
    FROM intermediate.cohort
),

live_sessions AS (
    SELECT 
        ls.id,
        ls.session_name,
        ls.code,
        ls.conducted_on::date AS conducted_on
    FROM intermediate.live_session ls
    JOIN cohort_range cr
        ON ls.conducted_on::date BETWEEN cr.start_date AND cr.end_date
),

student_attendance AS (
    SELECT 
        sd.student_id,
        gis."Incubator_Batch" AS incubator_batch,
        sdet.location_id,
        ls.id AS session_id,
        ls.session_name AS title,
        ls.code,
        ls.conducted_on,
        sd.duration_in_sec,
        --sd.education_course_id,
        COALESCE(sd.watched_on::date, ls.conducted_on) AS attended_on
    FROM intermediate.student_session sd
    JOIN live_sessions ls
        ON sd.session_id = ls.id
    JOIN intermediate.student_details sdet
        ON sd.student_id = sdet.id
    JOIN raw.general_information_sheet gis
        ON sdet.email = gis."Email"
),

student_registration AS (
    SELECT
        student_id,
        form_details
    FROM intermediate.student_registration_details
),

mapped_subjects AS (
    SELECT 
        se.student_id,
        se.education_course_id,
        cm.course_name,
        colm.standard_college_names AS college_name,
        um.standard_university_names AS university_name,
        sm.education_category,
        sm.subject_area,
        sm.sub_field
    FROM intermediate.student_education se
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id) ON TRUE      --unnest() function will create one row for each element in the array subject_id
    JOIN intermediate.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN intermediate.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN intermediate.college_mapping colm                              -- Left join as we don't have all the colleges listed in our standardized table i.e college_mapping table
        ON se.college_id = colm.college_id
    LEFT JOIN intermediate.university_mapping um                             -- Left join as we don't have all the universities listed in our standardized table i.e university_mapping table                
        ON se.university_id = um.university_id                               
),

aggregated_subjects AS (
    SELECT
        student_id,
        education_course_id,
        string_agg(DISTINCT education_category, ', ') AS education_category,
        string_agg(DISTINCT subject_area, ', ') AS subject_areas,
        string_agg(DISTINCT sub_field, ', ') AS sub_fields_list
    FROM mapped_subjects
    GROUP BY student_id, education_course_id
),

non_aggregated AS (
    SELECT DISTINCT
        student_id,
        education_course_id,
        course_name,
        college_name,
        university_name
    FROM mapped_subjects
)

SELECT
	TRIM(TO_CHAR(sa.attended_on, 'Day')) AS weekday_name,
    sa.student_id,
    sa.incubator_batch,
    sa.session_id,
    sa.title,
    sa.code,
    sa.conducted_on,
    sa.attended_on,
    sa.duration_in_sec,
    sr.form_details,
    lm.state_union_territory,
    lm.district,
    lm.country,
    lm.city_category,
    asub.education_category,
    asub.subject_areas,
    asub.sub_fields_list,
    na.course_name,
    na.college_name,
    na.university_name
FROM student_attendance sa
LEFT JOIN intermediate.location_mapping lm                      --INNER JOIN would have exclude INC 1,2,3 records because no match would be found in the standardized file.
    ON sa.location_id = lm.location_id
LEFT JOIN student_registration sr                               --Ensures no data from the student details table is lost, even if there is no corresponding entry in the student_registration table.
    ON sa.student_id = sr.student_id
LEFT JOIN aggregated_subjects asub
    ON sa.student_id = asub.student_id
LEFT JOIN non_aggregated na
    ON sa.student_id = na.student_id
    AND asub.education_course_id = na.education_course_id
--ORDER BY sa.conducted_on, sa.title;

""")

student_assignment_body = textwrap.dedent("""
WITH student_assignment AS (
    SELECT
        sd.id,
        sd.student_id,
        sd.cohort_code,
		sd.submitted_at,
		sd.resource_id AS student_resource_id,
		sd.submission_status,
        gs.id AS resource_id,
        gs.category,
        gs.title,
        gis."Incubator_Batch",
        sds.location_id
    FROM intermediate.student_assignment sd
    JOIN intermediate.resource gs
        ON sd.resource_id = gs.id
    JOIN intermediate.student_details sds
        ON sd.student_id = sds.id
    JOIN raw.general_information_sheet gis
        ON sds.email = gis."Email"
                                          
),

student_registration AS (
    SELECT
        student_id,
        form_details
    FROM intermediate.student_registration_details
),

-- Step 1: Unnest subject_id array and join to mapping tables from DB
mapped_subjects AS (
    SELECT 
        se.student_id,
        se.education_course_id,
        cm.course_name,
        colm.standard_college_names AS college_name,
        um.standard_university_names AS university_name,
        sm.education_category,
        sm.subject_area,
        sm.sub_field
    FROM intermediate.student_education se
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id) ON TRUE
    JOIN intermediate.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN intermediate.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN intermediate.college_mapping colm                 -- Left join as we don't have all the colleges listed in our standardized table i.e college_mapping table
        ON se.college_id = colm.college_id
    LEFT JOIN intermediate.university_mapping um                -- Left join as we don't have all the universities listed in our standardized table i.e university_mapping table
        ON se.university_id = um.university_id
),

-- Step 2: Aggregate only the subject-related fields
aggregated_subjects AS (
    SELECT
        student_id,
        education_course_id,
        string_agg(DISTINCT education_category, ', ') AS education_category,
        string_agg(DISTINCT subject_area, ', ') AS subject_areas,
        string_agg(DISTINCT sub_field, ', ') AS sub_fields_list
    FROM mapped_subjects
    GROUP BY student_id, education_course_id
),

-- Step 3: Non-aggregated fields (1:1 with student/course)
non_aggregated AS (
    SELECT DISTINCT
        student_id,
        education_course_id,
        course_name,
        college_name,
        university_name
    FROM mapped_subjects
)

-- Step 4: Final join
SELECT
    ss.student_id,
    ss."Incubator_Batch",
    ss.student_resource_id AS resource_id,
    ss.category,
    ss.title,
    ss.cohort_code,
    ss.submission_status,
	ss.submitted_at,
    sr.form_details,
    lm.state_union_territory,
    lm.district,
    lm.country,
    lm.city_category,
    asub.education_category,
    asub.subject_areas,
    asub.sub_fields_list,
    na.course_name,
    na.college_name,
    na.university_name
FROM student_assignment ss
LEFT JOIN intermediate.location_mapping lm                      --INNER JOIN would have exclude INC 1,2,3 records because no match would be found in the standardized file.
    ON ss.location_id = lm.location_id
LEFT JOIN student_registration sr                               --Ensures no data from the student details table is lost, even if there is no corresponding entry in the student_registration table.
    ON ss.student_id = sr.student_id
LEFT JOIN aggregated_subjects asub
    ON ss.student_id = asub.student_id
LEFT JOIN non_aggregated na
    ON ss.student_id = na.student_id
    AND asub.education_course_id = na.education_course_id;
""")

student_quiz_body = textwrap.dedent("""
WITH student_quiz AS (
    SELECT
        sd.id,
        sd.student_id,
        sd.cohort_code,
		sd.resource_id AS student_resource_id,
		sd.marks,
		sd.max_marks,
        gs.id AS resource_id,
        gs.category,
        gs.title,
        gis."Incubator_Batch",
        sds.location_id
    FROM intermediate.student_quiz sd
    JOIN intermediate.resource gs
        ON sd.resource_id = gs.id
    JOIN intermediate.student_details sds
        ON sd.student_id = sds.id
    JOIN raw.general_information_sheet gis
        ON sds.email = gis."Email"
),

student_registration AS (
    SELECT
        student_id,
        form_details
    FROM intermediate.student_registration_details
),

--Unnest subject_id array and join to mapping tables from DB
mapped_subjects AS (
    SELECT 
        se.student_id,
        se.education_course_id,
        cm.course_name,
        colm.standard_college_names AS college_name,
        um.standard_university_names AS university_name,
        sm.education_category,
        sm.subject_area,
        sm.sub_field
    FROM intermediate.student_education se
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id) ON TRUE
    JOIN intermediate.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN intermediate.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN intermediate.college_mapping colm                     -- Left join as we don't have all the colleges listed in our standardized table i.e college_mapping table
        ON se.college_id = colm.college_id
    LEFT JOIN intermediate.university_mapping um                    -- Left join as we don't have all the universities listed in our standardized table i.e university_mapping table
        ON se.university_id = um.university_id
),

-- Aggregate only the subject-related fields
aggregated_subjects AS (
    SELECT
        student_id,
        education_course_id,
        string_agg(DISTINCT education_category, ', ') AS education_category,
        string_agg(DISTINCT subject_area, ', ') AS subject_areas,
        string_agg(DISTINCT sub_field, ', ') AS sub_fields_list
    FROM mapped_subjects
    GROUP BY student_id, education_course_id
),

-- Non-aggregated fields (1:1 with student/course)
non_aggregated AS (
    SELECT DISTINCT
        student_id,
        education_course_id,
        course_name,
        college_name,
        university_name
    FROM mapped_subjects
)

-- Final join
SELECT
    ss.student_id,
    ss."Incubator_Batch",
    ss.id,
	ss.student_resource_id AS resource_id,
    ss.category,
    ss.title,
    ss.cohort_code,
    ss.marks,
	ss.max_marks,
    sr.form_details,
    lm.state_union_territory,
    lm.district,
    lm.country,
    lm.city_category,
    asub.education_category,
    asub.subject_areas,
    asub.sub_fields_list,
    na.course_name,
    na.college_name,
    na.university_name
FROM student_quiz ss
LEFT JOIN intermediate.location_mapping lm                      --INNER JOIN would have exclude INC 1,2,3 records because no match would be found in the standardized file.
    ON ss.location_id = lm.location_id
LEFT JOIN student_registration sr                               --Ensures no data from the student details table is lost, even if there is no corresponding entry in the student_registration table.
    ON ss.student_id = sr.student_id
LEFT JOIN aggregated_subjects asub
    ON ss.student_id = asub.student_id
LEFT JOIN non_aggregated na
    ON ss.student_id = na.student_id
    AND asub.education_course_id = na.education_course_id;
""")

# Map final table names
table_map = [
    ("student_demography", student_demography_body),
    ("daily_weekly_attendance", student_attendance_body),
    ("final_assignment", student_assignment_body),
    ("final_quiz", student_quiz_body),
]


def drop_table_if_exists(conn, schema: str, table: str):
    q = text(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;")
    conn.execute(q)
    print(f"Dropped table if existed: {schema}.{table}")

def run():
    with engine.begin() as conn:
        # ensure target schema exists
        try:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))
            print(f"Schema ensured: {TARGET_SCHEMA}")
        except Exception as e:
            print(f"Warning: could not create/ensure schema {TARGET_SCHEMA}: {e}")

        for table_name, body in table_map:
            full_sql = wrap_create(TARGET_SCHEMA, table_name, body)
            print(f"\n--- Preparing to create {TARGET_SCHEMA}.{table_name} ---")
            
            if DROP_IF_EXISTS:
                drop_table_if_exists(conn, TARGET_SCHEMA, table_name)
            try:
                print(f"Creating table {TARGET_SCHEMA}.{table_name} ...")
                conn.execute(text(full_sql))
                print(f"Created {TARGET_SCHEMA}.{table_name} successfully.")
            except Exception as e:
                print(f"Error creating {TARGET_SCHEMA}.{table_name}: {e}")
                # continue to next table
    print("\nAll operations completed.")

if __name__ == "__main__":
    run()
