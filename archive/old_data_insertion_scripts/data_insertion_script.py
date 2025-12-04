#Python monitoring data insertion script

import os
import pandas as pd
from sqlalchemy import create_engine, text
from deployment_scripts.connection import get_engine, get_session, metadata

# SQLAlchemy engine
engine = get_engine()

# SQL with PostgreSQL-style casting to match table datatypes
programs_query = text("""
    SELECT 
        'INC'::intermediate.program_code_enum AS program_code,
        'Incubator'::VARCHAR(20) AS full_form,
        NULL::DATE AS start_date
    UNION ALL
    SELECT 
        'ACC'::intermediate.program_code_enum,
        'Accelerator'::VARCHAR(20),
        NULL::DATE
    UNION ALL
    SELECT 
        'STC'::intermediate.program_code_enum,
        'Stem Champion'::VARCHAR(20),
        NULL::DATE
""")

cohort_query = text("""
    SELECT
        program.program_code || RIGHT('000' || mapping."Cohort number"::TEXT, 3)::VARCHAR(6) AS cohort_code,
        program.program_code::intermediate.program_code_enum AS program_code,
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
    JOIN intermediate.program program
        ON LOWER(mapping."Program Name") = LOWER(program.full_form)
    """)

live_session_query = text("""
    WITH cohort_cte AS (
        SELECT
            cohort_code,
            cohort_name
        FROM intermediate.cohort
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
            c.cohort_code::VARCHAR(6) AS cohort_code,
            s.name::TEXT AS session_name,
            s.type::intermediate.session_type_enum,
            s.code::TEXT,
            s.duration_in_sec::INT AS duration_in_sec,
            TO_DATE(s.conducted_on, 'DD-MM-YY') AS conducted_on       ---ensures compatibility across environments (local and EC2), regardless of PostgreSQL’s datestyle as it raw schema date format is "DD/MM/YYYY"
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
        FROM intermediate.cohort
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
            session_name,
            cohort_code,
            code
        FROM intermediate.live_session
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
            "Category"::intermediate.resource_category AS category,
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
        FROM intermediate.cohort
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
        FROM intermediate.resource
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

student_assignment_query = text("""
    WITH cohort_data AS (
        SELECT
            cohort_code,
            cohort_name
        FROM intermediate.cohort 
    ),


    raw_general_info_data AS (
        SELECT
            "Incubator_Course_Name" AS cohort_name,
            "Student_id" AS student_id,
            "Email" as email
        FROM raw.general_information_sheet
    ),
    

    assignment_data AS (
        SELECT
            "assignment_name" AS name,
            "Email" AS email,
            "student_name" AS student_name,
            "submission_status" AS submission_status,
            "feedback_comments" As feedback,
            "submitted_at" AS submitted_at,
            "assignment_file" AS assignment_file
        FROM raw.assignment_monitoring_data
    ),


        
    resource_data AS (
        SELECT 
            id AS resource_id,
            title,                --VID01,VID02,Assignment,Quiz...
            total_duration AS watchtime_in_sec
        FROM intermediate.resource
        WHERE category = 'Assignment'  -- <-- Only select Assignment resources
    ),  


    student_assignment_data AS (
        SELECT
            g.student_id::INT AS student_id,
            r.resource_id::INT AS resource_id,
            NULL::INT AS mentor_id,
            c.cohort_code::VARCHAR(6) AS cohort_code,
            a.submission_status::intermediate.submission_status_enum AS submission_status,
            (CASE 
                WHEN a.submission_status = 'under review' THEN 30
                WHEN a.submission_status = 'reviewed' THEN 100
                WHEN a.submission_status = 'rejected' THEN 80
                ELSE 0
            END)::DECIMAL AS marks_pct,
            a.feedback::TEXT AS feedback_comments,
            a.submitted_at::TIMESTAMP AS submitted_at,
            a.assignment_file::TEXT AS assignment_file

        FROM assignment_data a
        INNER JOIN raw_general_info_data g
            ON a.email = g.email
        INNER JOIN cohort_data c
            ON g.cohort_name = c.cohort_name 
        INNER JOIN resource_data r
            ON a.name = r.title
    )

    SELECT * FROM student_assignment_data
                                
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
        FROM intermediate.cohort
    ),
    resource_data AS (
        SELECT 
            id AS resource_id,
            title,
            total_duration AS watchtime_in_min
        FROM intermediate.resource
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

student_cohort_query = text("""
    WITH raw_general_info_data AS (
        SELECT
            "Incubator_Course_Name" AS cohort_name,
            "Student_id" AS student_id,
            "Student_Role" AS student_role,
            "Email" AS email
        FROM old.general_information_sheet
    ),
    student_details_data AS (
        SELECT
            id AS student_id,
            email AS email
        FROM raw.student_details
    ),
    cohort_data AS (
        SELECT
            cohort_code,
            cohort_name,
            start_date
        FROM raw.cohort
    ),
    student_cohort_data AS (
        SELECT
            RIGHT(CAST(EXTRACT(YEAR FROM c.start_date) AS TEXT), 2) || 
            c.cohort_code || 
            LPAD(CAST(sd.id AS TEXT), 7, '0') AS student_code,
            sd.id::INT AS student_id,
            c.cohort_code::VARCHAR(6) AS cohort_code,
            CASE 
                WHEN student_role ILIKE '%student leader%' THEN 'Yes'
                ELSE 'No'
            END AS is_leader,
            NULL AS cohort_enroll_date
        FROM student_details sd
        INNER JOIN raw_general_info_data g
            ON sd.email = g.email
        INNER JOIN cohort_data c
            ON g.cohort_name = c.cohort_name
    )
    SELECT * FROM student_cohort_data
""")


try:
    # Load as DataFrame and Insert into target table (append only)
    #programs_df = pd.read_sql(programs_query, engine)
    #programs_df.to_sql("program", engine, if_exists="append", index=False,schema="intermediate")

    #cohort_df = pd.read_sql(cohort_query, engine) 
    #cohort_df.to_sql("cohort", engine, if_exists="append", index=False,schema="intermediate")

    #live_session_df = pd.read_sql(live_session_query, engine) 
    #live_session_df.to_sql("live_session", engine, if_exists="append", index=False, schema="intermediate")
    
    #student_live_df = pd.read_sql(student_session_query, engine)
    #student_live_df.to_sql("student_session", engine, if_exists="append", index=False, schema="intermediate")

    #resource_df = pd.read_sql(resource_query, engine)
    #resource_df.to_sql("resource", engine, if_exists="append", index=False, schema="intermediate")

    #student_pre_recorded_df = pd.read_sql(student_pre_recorded_query, engine)
    #student_pre_recorded_df.to_sql("student_pre_recorded", engine, if_exists="append", index=False, schema="intermediate")

    #quiz_df = pd.read_sql(student_quiz_query, engine)
    #quiz_df.to_sql("student_quiz", engine, if_exists="append", index=False, schema="intermediate")

    #student_assignment_df = pd.read_sql(student_assignment_query, engine)
    #student_assignment_df.to_sql("student_assignment", engine, if_exists="append", index=False, schema="intermediate")

    student_cohort_df = pd.read_sql(student_cohort_query, engine)
    student_cohort_df.to_sql("student_cohort", engine, if_exists="append", index=False, schema="raw")

    print("Data inserted successfully into tables.")

except Exception as e:
    print("Error during insertion:", e)