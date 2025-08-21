import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load .env file
load_dotenv(r"deployment_scripts\config.env")

# Read DB credentials
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME")

# SQLAlchemy engine
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")


# Query to insert Incubator 7.0 student assignments
student_assignment_query = text("""
    WITH cohort_data AS (
        SELECT cohort_code, cohort_name FROM intermediate.cohort 
    ),
    raw_general_info_data AS (
        SELECT "Incubator_Course_Name" AS cohort_name, "Student_id" AS student_id, "Email" as email
        FROM raw.general_information_sheet
    ),
    assignment_data AS (
        SELECT
            "assignment_name" AS name,
            "Email" AS email,
            "student_name" AS student_name,
            "submission_status" AS submission_status,
            "feedback_comments" AS feedback,
            "submitted_at" AS submitted_at,
            "assignment_file" AS assignment_file
        FROM raw.assignment_monitoring_data
    ),
    resource_data AS (
        SELECT id AS resource_id, title
        FROM intermediate.resource
        WHERE category = 'Assignment'
    ),
    student_assignment_data AS (
        SELECT
            g.student_id::INT AS student_id,
            r.resource_id::INT AS resource_id,
            NULL::INT AS mentor_id,
            c.cohort_code AS cohort_code,
            a.submission_status::intermediate.submission_status_enum AS submission_status,
            (CASE 
                WHEN a.submission_status = 'under review' THEN 30
                WHEN a.submission_status = 'reviewed' THEN 100
                WHEN a.submission_status = 'rejected' THEN 80
                ELSE 0
            END)::DECIMAL AS marks_pct,
            a.feedback AS feedback_comments,
            a.submitted_at::TIMESTAMP AS submitted_at,
            a.assignment_file AS assignment_file
        FROM assignment_data a
        INNER JOIN raw_general_info_data g ON a.email = g.email
        INNER JOIN cohort_data c ON g.cohort_name = c.cohort_name 
        INNER JOIN resource_data r ON a.name = r.title
    )
    INSERT INTO intermediate.student_assignment (
        student_id, resource_id, mentor_id, cohort_code, submission_status,
        marks_pct, feedback_comments, submitted_at, assignment_file
    )
    SELECT 
        student_id, resource_id, mentor_id, cohort_code, submission_status,
        marks_pct, feedback_comments, submitted_at, assignment_file
    FROM student_assignment_data
    ON CONFLICT (student_id, resource_id, submitted_at)
    DO UPDATE SET
        mentor_id = EXCLUDED.mentor_id,
        cohort_code = EXCLUDED.cohort_code,
        submission_status = EXCLUDED.submission_status,
        marks_pct = EXCLUDED.marks_pct,
        feedback_comments = EXCLUDED.feedback_comments,
        submitted_at = EXCLUDED.submitted_at,
        assignment_file = EXCLUDED.assignment_file;
""")


student_session_query = text("""
    WITH cohort_data AS (
        SELECT cohort_code, cohort_name FROM intermediate.cohort
    ),
    raw_general_info_data AS (
        SELECT "Incubator_Course_Name" AS cohort_name, "Student_id" AS student_id, "Email" AS email
        FROM raw.general_information_sheet
    ),
    session_data AS (
        SELECT id AS session_id, session_name, cohort_code, code
        FROM intermediate.live_session
    ),
    raw_student_session_info AS (
        SELECT
            "Email" AS email,
            "Session_Code" AS session_code,
            "Duration_in_secs" AS duration_in_sec,
            "watched_on" AS watched_on
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
            ssi.watched_on::DATE AS watched_on
        FROM raw_student_session_info ssi
        INNER JOIN raw_general_info_data g ON ssi.email = g.email
        INNER JOIN cohort_data c ON g.cohort_name = c.cohort_name
        INNER JOIN session_data s ON ssi.session_code = s.code AND c.cohort_code = s.cohort_code
    )
    INSERT INTO intermediate.student_session (
        student_id, session_id, duration_in_sec, watched_on
    )
    SELECT * FROM student_live_session_cte
    ON CONFLICT (student_id, session_id)
    DO UPDATE SET
        duration_in_sec = EXCLUDED.duration_in_sec,
        watched_on = EXCLUDED.watched_on;
""")

student_quiz_query = text("""
    WITH raw_general_info_data AS (
        SELECT "Incubator_Course_Name" AS cohort_name, "Student_id" AS student_id, "Email" AS email
        FROM raw.general_information_sheet
    ),
    quiz_data AS (
        SELECT
            "user_id" AS email,
            "data_fields" AS quiz_name,
            "value" AS obtained_marks
        FROM raw.incubator_quiz_monitoring
    ),
    cohort_data AS (
        SELECT cohort_code, cohort_name FROM intermediate.cohort
    ),
    resource_data AS (
        SELECT id AS resource_id, title
        FROM intermediate.resource
        WHERE category = 'Quiz'
    ),
    student_quiz_data AS (
        SELECT
            g.student_id::INT AS student_id,
            r.resource_id::INT AS resource_id,
            c.cohort_code AS cohort_code,
            100::INT AS max_marks,
            q.obtained_marks::INT AS marks,
            NULL::INT AS reattempts,
            NULL::TIMESTAMP AS attempted_at
        FROM quiz_data q
        INNER JOIN raw_general_info_data g ON q.email = g.email
        INNER JOIN cohort_data c ON g.cohort_name = c.cohort_name
        INNER JOIN resource_data r ON q.quiz_name = r.title
    )
    INSERT INTO intermediate.student_quiz (
        student_id, resource_id, cohort_code, max_marks, marks, reattempts, attempted_at
    )
    SELECT * FROM student_quiz_data
    ON CONFLICT (student_id, resource_id)
    DO UPDATE SET
        cohort_code = EXCLUDED.cohort_code,
        max_marks = EXCLUDED.max_marks,
        marks = EXCLUDED.marks,
        reattempts = EXCLUDED.reattempts,
        attempted_at = EXCLUDED.attempted_at;
""")


# Execute queries

if __name__ == "__main__":
    print("\nChoose an option:")
    print("1. Insert into student_assignment")
    print("2. Insert into student_session")
    print("3. Insert into student_quiz")
    
    choice = input("Enter your choice (1/2/3): ").strip()

    with engine.begin() as conn:
        if choice == "1":
            assignment_result = conn.execute(student_assignment_query)
            print("* Data appended to 'student_assignment' table.")
            print(f"   - Rows inserted/updated: {assignment_result.rowcount}")
        
        elif choice == "2":
            session_result = conn.execute(student_session_query)
            print("* Data appended to 'student_session' table.")
            print(f"   - Rows inserted/updated: {session_result.rowcount}")
        
        elif choice == "3":
            quiz_result = conn.execute(student_quiz_query)
            print("* Data appended to 'student_quiz' table.")
            print(f"   - Rows inserted/updated: {quiz_result.rowcount}")
        
        else:
            print("# Invalid choice. Please enter 1, 2, or 3.")
