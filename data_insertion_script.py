#Python monitoring data insertion script

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

#Load .env file
load_dotenv("configuration.env")

#Read DB credentials
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME")

# SQLAlchemy engine
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

# SQL with PostgreSQL-style casting to match table datatypes
programs_query = text("""
    SELECT 
        'INC'::program_code_enum AS program_code,
        'Incubator'::VARCHAR(20) AS full_form,
        NULL::DATE AS start_date
    UNION ALL
    SELECT 
        'ACC'::program_code_enum,
        'Accelerator'::VARCHAR(20),
        NULL::DATE
    UNION ALL
    SELECT 
        'STC'::program_code_enum,
        'Stem Champion'::VARCHAR(20),
        NULL::DATE
""")

cohort_query = text("""
    SELECT
        program.program_code || RIGHT('000' || mapping."Cohort number"::TEXT, 3)::VARCHAR(6) AS cohort_code,
        program.program_code::program_code_enum AS program_code,
        mapping."Cohort number"::INT AS cohort_number,
        mapping."Cohort Name"::VARCHAR(300) AS cohort_name,
        mapping."Type"::TEXT AS type,
        TO_DATE(mapping."Start Date", 'DD-MM-YYYY') AS start_date,      ---ensures compatibility across environments (local and EC2), regardless of PostgreSQL’s datestyle
        TO_DATE(mapping."End Date", 'DD-MM-YYYY') AS end_date,
        (CASE 
            WHEN TO_DATE(mapping."End Date", 'DD-MM-YYYY') >= CURRENT_DATE THEN 'Yes'
            ELSE 'No'
        END)::BOOLEAN AS is_active
    FROM raw.cohort_details mapping
    JOIN vg_prod.program program
        ON LOWER(mapping."Program Name") = LOWER(program.full_form)
    """)

live_session_query = text("""
    WITH cohort_cte AS (
        SELECT
            cohort_code,
            cohort_name
        FROM vg_prod.cohort
    ),
    session_cte AS (
        SELECT 
            "Session_Name" AS name,
            "Type" AS type,
            "Session Code" AS code,
            "Cohort_Name" AS cohort_name,
            "Duration" AS duration_in_sec,
            "Conducted_On" AS conducted_on
        FROM raw.session_details
    ),
    live_session_cte AS (
        SELECT
            c.cohort_code::VARCHAR(6),
            s.name::TEXT,
            s.type::session_type_enum,
            s.code::TEXT,
            s.duration_in_sec::INT,
            TO_DATE(s.conducted_on, 'DD-MM-YYYY') AS conducted_on       ---ensures compatibility across environments (local and EC2), regardless of PostgreSQL’s datestyle as it raw schema date format is "DD/MM/YYYY"
        FROM session_cte s
        INNER JOIN cohort_cte c
            ON LOWER(TRIM(s.cohort_name)) = LOWER(TRIM(c.cohort_name))
    )
    SELECT * FROM live_session_cte
""")

student_session_query = text("""
    WITH cohort_data AS (
        SELECT
            cohort_code,
            cohort_name
        FROM vg_prod.cohort
    ),
    raw_general_info_data AS (
        SELECT
            "Incubator_Course_Name" AS cohort_name,
            "Student_id" AS student_id,
            "Email" AS email
        FROM raw.general_information_sheet
    ),
    session_data AS (
        SELECT
            id AS session_id,
            name,
            cohort_code,
            code
        FROM vg_prod.live_session
    ),
    raw_student_session_info AS (
        SELECT
            "Email" AS email,
            "Session_Code" AS session_code,
            "Duration_in_secs" AS duration_in_sec
        FROM raw.student_session_information
        WHERE "Session_Code" LIKE 'SUK%' 
           OR "Session_Code" LIKE 'WS%'      
           OR "Session_Code" LIKE 'MC%'
    ),
    student_live_session_cte AS (
        SELECT
            g.student_id::INT AS student_id,
            s.session_id::INT AS session_id,
            ssi.duration_in_sec::INT AS duration_in_sec,
            Null::DATE AS watched_on
        FROM raw_student_session_info ssi
        INNER JOIN raw_general_info_data g
            ON ssi.email = g.email
        INNER JOIN cohort_data c        
            ON g.cohort_name = c.cohort_name
        INNER JOIN session_data s         
            ON ssi.session_code = s.code
           AND c.cohort_code = s.cohort_code
    )
    SELECT * FROM student_live_session_cte
""")
resource_query = text("""
    WITH resource_cte AS (
        SELECT 
            "Category"::resource_category AS category,
            "Title"::VARCHAR(300) AS title,
            "Content Name"::TEXT AS description,
            NULL::TEXT AS location,
            NULL::TEXT AS resource_link,
            "Is_Video"::BOOLEAN AS is_video_resource,
            "Time"::INT AS total_duration
        FROM raw.resource_details
    )
    SELECT * FROM resource_cte
""")

student_pre_recorded_query = text("""
    WITH cohort_data AS (
        SELECT
            cohort_code,
            cohort_name
        FROM vg_prod.cohort
    ),
    raw_general_info_data AS (
        SELECT
            "Incubator_Course_Name" AS cohort_name,
            "Student_id" AS student_id,
            "Email" AS email
        FROM raw.general_information_sheet
    ),
    resource_data AS (
        SELECT 
            id AS resource_id,
            title,
            total_duration AS watchtime_in_sec
        FROM vg_prod.resource
    ),
    student_session_info AS (
        SELECT
            "Email" AS email,
            "Session_Code" AS session_code,
            "Duration_in_secs" AS watchtime_in_secs
        FROM raw.student_session_information
        WHERE "Session_Code" LIKE 'VID%' 
    ),
    student_pre_recorded_data AS (
        SELECT                     
            g.student_id::INT AS student_id,
            r.resource_id::INT AS resource_id,
            c.cohort_code::VARCHAR(6) AS cohort_code,
            s.watchtime_in_secs::INT AS watchtime_in_sec,
            NULL::TIMESTAMP AS watched_at
        FROM student_session_info s
        INNER JOIN raw_general_info_data g
            ON s.email = g.email
        INNER JOIN cohort_data c
            ON g.cohort_name = c.cohort_name
        INNER JOIN resource_data r
            ON s.session_code = r.title
    )
    SELECT * FROM student_pre_recorded_data
""")

student_quiz_query = text("""
    WITH raw_general_info_data AS (
        SELECT
            "Incubator_Course_Name" AS cohort_name,
            "Student_id" AS student_id,
            "Email" AS email
        FROM raw.general_information_sheet
    ),
    quiz_data AS (
        SELECT
            "user_id" AS email,
            "data_fields" AS quizes_name,
            "value" AS obtained_marks
        FROM raw.incubator_quiz_monitoring
    ),
    cohort_data AS (
        SELECT
            cohort_code,
            cohort_name
        FROM vg_prod.cohort
    ),
    resource_data AS (
        SELECT 
            id AS resource_id,
            title,
            total_duration AS watchtime_in_min
        FROM vg_prod.resource
        WHERE category = 'Quiz'
    ),
    student_quiz_data AS (
        SELECT
            g.student_id::INT AS student_id,
            r.resource_id::INT AS resource_id,
            c.cohort_code::VARCHAR(6) AS cohort_code,
            100::INT AS max_marks,
            q.obtained_marks::INT AS marks,
            NULL::INT AS reattempts,
            NULL::TIMESTAMP AS attempted_at
        FROM quiz_data q
        INNER JOIN raw_general_info_data g
            ON q.email = g.email
        INNER JOIN cohort_data c
            ON g.cohort_name = c.cohort_name
        INNER JOIN resource_data r
            ON q.quizes_name = r.title
    )
    SELECT * FROM student_quiz_data
""")

try:
    # Load as DataFrame
    programs_df = pd.read_sql(programs_query, engine)
    cohort_df = pd.read_sql(cohort_query, engine) 
    live_session_df = pd.read_sql(live_session_query, engine) 
    #print(live_session_df.shape)
    #print(live_session_df['cohort_code'].unique())
    student_live_df = pd.read_sql(student_session_query, engine)
    resource_df = pd.read_sql(resource_query, engine)
    student_pre_recorded_df = pd.read_sql(student_pre_recorded_query, engine)
    quiz_df = pd.read_sql(student_quiz_query, engine)

    # Insert into target table (append only)
    programs_df.to_sql("program", engine, if_exists="append", index=False,schema="vg_prod")
    cohort_df.to_sql("cohort", engine, if_exists="append", index=False,schema="vg_prod")
    live_session_df.to_sql("live_session", engine, if_exists="append", index=False, schema="vg_prod")
    student_live_df.to_sql("student_session", engine, if_exists="append", index=False, schema="vg_prod")
    resource_df.to_sql("resource", engine, if_exists="append", index=False, schema="vg_prod")
    student_pre_recorded_df.to_sql("student_pre_recorded", engine, if_exists="append", index=False, schema="vg_prod")
    quiz_df.to_sql("student_quiz", engine, if_exists="append", index=False, schema="vg_prod")

    print("Data inserted successfully into 'program, cohort, live_session, student_live_session, resource, student_pre_recorded, student_quiz'.")

except Exception as e:
    print("Error during insertion:", e)
