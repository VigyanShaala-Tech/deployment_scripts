import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config.env")

# DB credentials
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME")

# SQLAlchemy engine
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

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
    FROM raw.general_information_sheet
    GROUP BY "Email"
    HAVING COUNT(*) > 1;
    """
    with engine.begin() as conn:
        dup_df = pd.read_sql(dup_query, conn)
        if not dup_df.empty:
            dup_df.to_csv("duplicate_general_information_sheet.csv", index=False)
            print("* Duplicates exported to duplicate_general_information_sheet.csv")
            
            delete_query = """
            DELETE FROM raw.general_information_sheet a
            USING raw.general_information_sheet b
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

student_demography_upsert_query = text("""
WITH student_details AS (
    SELECT
        sd.id,
        sd.email,
        sd.caste,
        sd.annual_family_income_inr,
        sd.location_id,
        gs."Incubator_Batch"
    FROM intermediate.student_details sd
    JOIN raw.general_information_sheet gs
        ON sd.email = gs."Email"
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
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id) ON TRUE
    JOIN intermediate.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN intermediate.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN intermediate.college_mapping colm
        ON se.college_id = colm.college_id
    LEFT JOIN intermediate.university_mapping um
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
INSERT INTO final.student_demography (
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
LEFT JOIN intermediate.location_mapping lm
    ON sd.location_id = lm.location_id
LEFT JOIN student_registration sr
    ON sd.id = sr.student_id
LEFT JOIN aggregated_subjects asub
    ON sd.id = asub.student_id
LEFT JOIN non_aggregated na
    ON sd.id = na.student_id
    AND asub.education_course_id = na.education_course_id
ON CONFLICT (email)
DO UPDATE SET
    student_id = EXCLUDED.student_id,
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
    university_name = EXCLUDED.university_name;
""")


# -------------------------------
# Quiz upsert query
# -------------------------------
quiz_upsert_query = text("""
WITH student_quiz AS (
    SELECT
        sd.id,
        sd.student_id,
        sd.cohort_code,
        sd.resource_id,
        sd.marks,
        sd.max_marks,
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
    SELECT student_id, form_details
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
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id) ON TRUE
    JOIN intermediate.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN intermediate.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN intermediate.college_mapping colm
        ON se.college_id = colm.college_id
    LEFT JOIN intermediate.university_mapping um
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
INSERT INTO final.final_quiz (
    id,
    resource_id,
    student_id,
    "Incubator_Batch",
    category,
    title,
    cohort_code,
    marks,
    max_marks,
    form_details,
    state_union_territory,
    district,
    country,
    city_category,
    education_category,
    subject_areas,
    sub_fields_list,
    course_name,
    college_name,
    university_name
)
SELECT
    ss.id,
    ss.resource_id,
    ss.student_id,
    ss."Incubator_Batch",
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
LEFT JOIN intermediate.location_mapping lm
    ON ss.location_id = lm.location_id
LEFT JOIN student_registration sr
    ON ss.student_id = sr.student_id
LEFT JOIN aggregated_subjects asub
    ON ss.student_id = asub.student_id
LEFT JOIN non_aggregated na
    ON ss.student_id = na.student_id
    AND asub.education_course_id = na.education_course_id
ON CONFLICT (student_id, resource_id)
DO UPDATE SET
    student_id = EXCLUDED.student_id,
    "Incubator_Batch" = EXCLUDED."Incubator_Batch",
    category = EXCLUDED.category,
    title = EXCLUDED.title,
    cohort_code = EXCLUDED.cohort_code,
    marks = EXCLUDED.marks,
    max_marks = EXCLUDED.max_marks,
    form_details = EXCLUDED.form_details,
    state_union_territory = EXCLUDED.state_union_territory,
    district = EXCLUDED.district,
    country = EXCLUDED.country,
    city_category = EXCLUDED.city_category,
    education_category = EXCLUDED.education_category,
    subject_areas = EXCLUDED.subject_areas,
    sub_fields_list = EXCLUDED.sub_fields_list,
    course_name = EXCLUDED.course_name,
    college_name = EXCLUDED.college_name,
    university_name = EXCLUDED.university_name;
""")

# -------------------------------
# Assignment upsert query
# -------------------------------
assignment_upsert_query = text("""
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
    SELECT student_id, form_details
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
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id) ON TRUE
    JOIN intermediate.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN intermediate.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN intermediate.college_mapping colm
        ON se.college_id = colm.college_id
    LEFT JOIN intermediate.university_mapping um
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
INSERT INTO final.final_assignment (
    student_id,
    "Incubator_Batch",
    resource_id,
    category,
    title,
    cohort_code,
    submission_status,
    submitted_at,
    form_details,
    state_union_territory,
    district,
    country,
    city_category,
    education_category,
    subject_areas,
    sub_fields_list,
    course_name,
    college_name,
    university_name
)
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
LEFT JOIN intermediate.location_mapping lm
    ON ss.location_id = lm.location_id
LEFT JOIN student_registration sr
    ON ss.student_id = sr.student_id
LEFT JOIN aggregated_subjects asub
    ON ss.student_id = asub.student_id
LEFT JOIN non_aggregated na
    ON ss.student_id = na.student_id
    AND asub.education_course_id = na.education_course_id
ON CONFLICT (student_id, resource_id, submitted_at)
DO UPDATE SET
    "Incubator_Batch" = EXCLUDED."Incubator_Batch",
    category = EXCLUDED.category,
    title = EXCLUDED.title,
    cohort_code = EXCLUDED.cohort_code,
    submission_status = EXCLUDED.submission_status,
    form_details = EXCLUDED.form_details,
    state_union_territory = EXCLUDED.state_union_territory,
    district = EXCLUDED.district,
    country = EXCLUDED.country,
    city_category = EXCLUDED.city_category,
    education_category = EXCLUDED.education_category,
    subject_areas = EXCLUDED.subject_areas,
    sub_fields_list = EXCLUDED.sub_fields_list,
    course_name = EXCLUDED.course_name,
    college_name = EXCLUDED.college_name,
    university_name = EXCLUDED.university_name;
""")

# -------------------------------
# Attendance upsert query
# -------------------------------

attendance_upsert_query = text("""
WITH cohort_range AS (
    SELECT start_date, end_date
    FROM intermediate.cohort
    WHERE cohort_code = 'INC007'
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
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id) ON TRUE
    JOIN intermediate.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN intermediate.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN intermediate.college_mapping colm
        ON se.college_id = colm.college_id
    LEFT JOIN intermediate.university_mapping um
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

INSERT INTO final.daily_weekly_attendance (
    weekday_name,
    student_id,
    session_id,
    incubator_batch,
    title,
    code,
    conducted_on,
    attended_on,
    duration_in_sec,
    form_details,
    state_union_territory,
    district,
    country,
    city_category,
    education_category,
    subject_areas,
    sub_fields_list,
    course_name,
    college_name,
    university_name
)
SELECT
    TRIM(TO_CHAR(sa.attended_on, 'Day')) AS weekday_name,  
    sa.student_id,
    sa.session_id,
    sa.incubator_batch,
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
LEFT JOIN intermediate.location_mapping lm
    ON sa.location_id = lm.location_id
LEFT JOIN student_registration sr
    ON sa.student_id = sr.student_id
LEFT JOIN aggregated_subjects asub
    ON sa.student_id = asub.student_id
LEFT JOIN non_aggregated na
    ON sa.student_id = na.student_id
   AND asub.education_course_id = na.education_course_id
ON CONFLICT (student_id, session_id)
DO UPDATE SET
    weekday_name = EXCLUDED.weekday_name,
    incubator_batch = EXCLUDED.incubator_batch,
    title = EXCLUDED.title,
    code = EXCLUDED.code,
    conducted_on = EXCLUDED.conducted_on,
    attended_on = EXCLUDED.attended_on,
    duration_in_sec = EXCLUDED.duration_in_sec,
    form_details = EXCLUDED.form_details,
    state_union_territory = EXCLUDED.state_union_territory,
    district = EXCLUDED.district,
    country = EXCLUDED.country,
    city_category = EXCLUDED.city_category,
    education_category = EXCLUDED.education_category,
    subject_areas = EXCLUDED.subject_areas,
    sub_fields_list = EXCLUDED.sub_fields_list,
    course_name = EXCLUDED.course_name,
    college_name = EXCLUDED.college_name,
    university_name = EXCLUDED.university_name;
""")

# -------------------------------
# Execute upserts
# -------------------------------

if __name__ == "__main__":

    with engine.begin() as conn:
        try:
            prepare_table_for_upsert("final.final_quiz", ["student_id", "resource_id"], "duplicate_final_quiz.csv")
            quiz_result = conn.execute(quiz_upsert_query)
            print("* Data upserted to 'final.final_quiz'.")
            print(f"   - Rows inserted/updated: {quiz_result.rowcount}")
        except Exception as e:
            print(f"! Failed to upsert into 'final.final_quiz': {e}")

        try:
            prepare_table_for_upsert("final.student_demography", ["email"], "duplicate_final_student_demography.csv")
            demography_result = conn.execute(student_demography_upsert_query)
            print("* Data upserted to 'final.student_demography'.")
            print(f"   - Rows inserted/updated: {demography_result.rowcount}")
        except Exception as e:
            print(f"! Failed to upsert into 'final.student_demography': {e}")

        try:
            prepare_table_for_upsert("final.final_assignment", ["student_id", "resource_id", "submitted_at"], "duplicate_final_assignment.csv")
            assign_result = conn.execute(assignment_upsert_query)
            print("* Data upserted to 'final.final_assignment'.")
            print(f"   - Rows inserted/updated: {assign_result.rowcount}")
        except Exception as e:
            print(f"! Failed to upsert into 'final.final_assignment': {e}")

        try:
            prepare_table_for_upsert("final.daily_weekly_attendance",["student_id", "session_id"],"duplicate_daily_weekly_student_attendance.csv")
            attendance_result = conn.execute(attendance_upsert_query)
            print("* Data upserted to 'final.daily_weekly_attendance'.")
            print(f"   - Rows inserted/updated: {attendance_result.rowcount}")
        except Exception as e:
            print(f"! Failed to upsert into 'final.daily_weekly_attendance': {e}")



'''if __name__ == "__main__":
    with engine.begin() as conn:
        # Quiz upsert
        quiz_result = conn.execute(quiz_upsert_query)
        print("* Data upserted to 'final.final_quiz'.")
        print(f"   - Rows inserted/updated: {quiz_result.rowcount}")

        # Assignment upsert
        assign_result = conn.execute(assignment_upsert_query)
        print("* Data upserted to 'final.final_assignment'.")
        print(f"   - Rows inserted/updated: {assign_result.rowcount}")

        # Attendance upsert
        attendance_result = conn.execute(attendance_upsert_query)
        print("* Data upserted to 'final.daily_weekly_attendance'.")
        print(f"   - Rows inserted/updated: {attendance_result.rowcount}")'''
