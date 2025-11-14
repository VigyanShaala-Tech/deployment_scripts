import os
from sqlalchemy import create_engine, text
from deployment_scripts.connection import get_engine, get_session, metadata

# Set schema name at the top
schema = "intermediate"

# Create SQLAlchemy engine
engine = get_engine()

# SQL: Create schema and table

create_student_details_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE IF NOT EXISTS {schema}.student_details (
        id SERIAL PRIMARY KEY,
        email VARCHAR(254),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        gender {schema}.gender_enum,
        phone VARCHAR(15),
        date_of_birth DATE,
        caste text,
        annual_family_income_inr TEXT,
        location_id INT,
        FOREIGN KEY (location_id) REFERENCES {schema}.location_mapping(location_id)
    );
""")

create_referral_college_professor_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE IF NOT EXISTS {schema}.referral_college_professor (
        id SERIAL PRIMARY KEY,
        student_id INT,
        college_id INT,
        name VARCHAR(50),
        phone VARCHAR(15),
        FOREIGN KEY (student_id) REFERENCES {schema}.student_details(id),
        FOREIGN KEY (college_id) REFERENCES {schema}.college_mapping(college_id)
    );
""")



create_student_registration_details_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE IF NOT EXISTS {schema}.student_registration_details (
        id SERIAL PRIMARY KEY,
        student_id INT,
        assigned_through TEXT,
        registration_date DATE,
        form_details JSONB,  -- Stores flexible form data
        FOREIGN KEY (student_id) REFERENCES {schema}.student_details(id)

    );
""")

create_student_education = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE IF NOT EXISTS {schema}.student_education(
        id SERIAL PRIMARY KEY,
        student_id INT,
        education_course_id INT,
        subject_id INT[],
        interest_subject_id INT,
        college_id INT,
        university_id INT,
        college_location_id INT,
        start_year INT,
        end_year INT,
        FOREIGN KEY (student_id) REFERENCES {schema}.student_details(id),
        FOREIGN KEY (education_course_id) REFERENCES {schema}.course_mapping(course_id),
        FOREIGN KEY (interest_subject_id) REFERENCES {schema}.subject_mapping(id),
        FOREIGN KEY (college_id) REFERENCES {schema}.college_mapping(college_id),
        FOREIGN KEY (college_location_id) REFERENCES {schema}.location_mapping(location_id)
    );
""")

create_program_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE IF NOT EXISTS {schema}.program (
        program_code {schema}.program_code_enum PRIMARY KEY,
        full_form VARCHAR(20),
        start_date DATE
    );
""")

create_cohort_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE IF NOT EXISTS {schema}.cohort (
        cohort_code VARCHAR(6) NOT NULL PRIMARY KEY,         -- e.g., INC001
        program_code {schema}.program_code_enum,                             -- e.g., INC, STC, ACC
        cohort_number INT,
        cohort_name VARCHAR(300),
        type text,                               -- enum: 'open' or 'curriculum'
        start_date DATE,
        end_date DATE,
        is_active BOOLEAN,

        -- Foreign key constraint linking to program table
        FOREIGN KEY (program_code) REFERENCES {schema}.program(program_code)

    );
""")

create_resource_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE IF NOT EXISTS {schema}.resource (
        id serial PRIMARY KEY,
        category {schema}.resource_category,
        title VARCHAR(300),
        description TEXT,
        location TEXT,
        resource_link TEXT,
        is_video_resource BOOLEAN,
        total_duration INT
                           
    );
""")

create_live_session_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE {schema}.live_session (
        id serial PRIMARY KEY,
        cohort_code VARCHAR(6),
        session_name TEXT,
        type {schema}.session_type_enum,  -- enum: masterclass, SUK, workshop
        code TEXT,
        duration_in_sec INT,
        conducted_on TIMESTAMP,
        FOREIGN KEY (cohort_code) REFERENCES {schema}.cohort(cohort_code)
    );
""")


create_student_session_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE {schema}.student_session (
        id SERIAL PRIMARY KEY,
        student_id INT,
        session_id INT,
        duration_in_sec INT,
        watched_on DATE,
        FOREIGN KEY (student_id) REFERENCES {schema}.student_details(id),
        FOREIGN KEY (session_id) REFERENCES {schema}.live_session(id)
    );
""")

create_student_quiz_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE IF NOT EXISTS {schema}.student_quiz (
        id SERIAL PRIMARY KEY,
        student_id INT,
        resource_id INT,
        cohort_code VARCHAR(6),
        marks INT,
        max_marks INT,
        reattempts INT,
        attempted_at TIMESTAMP,
        FOREIGN KEY (student_id) REFERENCES {schema}.student_details(id),
        FOREIGN KEY (resource_id) REFERENCES {schema}.resource(id),
        FOREIGN KEY (cohort_code) REFERENCES {schema}.cohort(cohort_code)
    );
""")

create_student_pre_recorded_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE {schema}.student_pre_recorded (
        id SERIAL PRIMARY KEY,
        student_id INT,
        resource_id INT,
        cohort_code VARCHAR(6),
        watchtime_in_sec INT,
        watched_at TIMESTAMP,

        FOREIGN KEY (student_id) REFERENCES {schema}.student_details(id),
        FOREIGN KEY (resource_id) REFERENCES {schema}.resource(id),
        FOREIGN KEY (cohort_code) REFERENCES {schema}.cohort(cohort_code)

    );
""")


create_mentor_details_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE {schema}.mentor_details (
        id serial PRIMARY KEY,
        name VARCHAR(50),
        email_id VARCHAR(254),
        linkedIn_url VARCHAR(200),
        current_position VARCHAR(100),
        phone_number VARCHAR(15),  -- Supports international formats
        mentor_role TEXT,  --- mentor roles are not yet defined will be defined during accelerator
        location_id INT,
        joined_on DATE,
        is_active BOOLEAN,
        FOREIGN KEY (location_id) REFERENCES {schema}.location_mapping(location_id)
    );
""")

create_student_assignment_table = text(f"""

    -- Create table with proper foreign keys
    CREATE TABLE {schema}.student_assignment (
        id SERIAL PRIMARY KEY,
        student_id INT,
        resource_id INT,
        mentor_id INT,
        cohort_code VARCHAR(6),
        submission_status {schema}.submission_status_enum,
        marks_pct DECIMAL,
        feedback_comments TEXT,
        submitted_at TIMESTAMP,
        assignment_file TEXT,  -- Consider replacing with TEXT or VARCHAR if storing file URL
        
        FOREIGN KEY (student_id) REFERENCES {schema}.student_details(id),
        FOREIGN KEY (resource_id) REFERENCES {schema}.resource(id),
        FOREIGN KEY (mentor_id) REFERENCES {schema}.mentor_details(id),
        FOREIGN KEY (cohort_code) REFERENCES {schema}.cohort(cohort_code)
    );
""")


# Run the creation script
try:
    with engine.begin() as conn:  # begin() ensures transaction safety
        conn.execute(create_student_details_table)
        conn.execute(create_referral_college_professor_table)
        conn.execute(create_student_registration_details_table)
        conn.execute(create_student_education)
        conn.execute(create_program_table)
        conn.execute(create_cohort_table)
        conn.execute(create_resource_table)
        conn.execute(create_live_session_table)
        conn.execute(create_student_session_table)
        conn.execute(create_student_pre_recorded_table)
        conn.execute(create_student_quiz_table)
        conn.execute(create_mentor_details_table)
        conn.execute(create_student_assignment_table)


        print("✅ table created successfully.")
except Exception as e:
    import traceback
    traceback.print_exc()
    print("❌ Error creating table:", e)