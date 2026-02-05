import os
from sqlalchemy import create_engine, text
from deployment_scripts.connection import get_engine, get_session, metadata

engine = get_engine()

insert_students_sql = text("""
INSERT INTO raw.student_details (
    first_name,
    last_name,
    gender,
    caste,
    annual_family_income_inr,
    inserted_at,
    updated_at,
    email
)
SELECT
    CASE
        WHEN full_name LIKE '%.%'
            THEN trim(split_part(full_name, '.', 2))
        ELSE trim(regexp_replace(full_name, '\\s+\\S+$', ''))
    END AS first_name,

    CASE
        WHEN full_name LIKE '%.%'
            THEN split_part(full_name, '.', 1)
        ELSE split_part(full_name, ' ', -1)
    END AS last_name,

    'F',
    caste,
    annual_family_income,
    NOW() AT TIME ZONE 'Asia/Kolkata',
    NOW() AT TIME ZONE 'Asia/Kolkata',
    NULL
FROM public.uploadsdata_insertion_20260102030852;
""")


insert_registration_sql = text("""
INSERT INTO raw.student_registration_details (
    student_id,
    assigned_through,
    form_details,
    registration_date,
    inserted_at,
    updated_at
)
SELECT
    sd.id AS student_id,
    u.assigned_through,
    jsonb_build_object(
        'currently_pursuing_year',
        u.currently_pursuing_year
    ) AS form_details,
    u.registration_date,
    NOW() AT TIME ZONE 'Asia/Kolkata',
    NOW() AT TIME ZONE 'Asia/Kolkata'
FROM raw.student_details_copy sd
JOIN public.uploadsdata_insertion_20260102030852 u
    ON sd.id = u.student_id;
""")


insert_education_sql = text(r"""
INSERT INTO raw.student_education (
    student_id,
    education_course_id,
    subject_id,
    college_id,
    university_id,
    college_location_id,
    start_year,
    end_year,
    inserted_at,
    updated_at
)
SELECT
    sd.id AS student_id,
    cmc.course_id AS education_course_id,
    ARRAY_AGG(DISTINCT sm.id ORDER BY sm.id) AS subject_id,
    cm.college_id,
    um.university_id,
    lm.location_id AS college_location_id,

    -- start_year
    (
        CASE
            WHEN CURRENT_DATE <
                 make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, 7, 1)
                THEN EXTRACT(YEAR FROM CURRENT_DATE)::int - 1
            ELSE EXTRACT(YEAR FROM CURRENT_DATE)::int
        END
        -
        (
            LEAST(
                COALESCE(
                    NULLIF(regexp_replace(u.currently_pursuing_year, '\D', '', 'g'), '')::int,
                    1
                ),
                cmc.course_duration
            ) - 1
        )
    )::int AS start_year,

    -- end_year
    (
        (
            CASE
                WHEN CURRENT_DATE <
                     make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, 7, 1)
                    THEN EXTRACT(YEAR FROM CURRENT_DATE)::int - 1
                ELSE EXTRACT(YEAR FROM CURRENT_DATE)::int
            END
            -
            (
                LEAST(
                    COALESCE(
                        NULLIF(regexp_replace(u.currently_pursuing_year, '\D', '', 'g'), '')::int,
                        1
                    ),
                    cmc.course_duration
                ) - 1
            )
        )
        + cmc.course_duration
    )::int AS end_year,

    NOW() AT TIME ZONE 'Asia/Kolkata' AS inserted_at,
    NOW() AT TIME ZONE 'Asia/Kolkata' AS updated_at
FROM raw.student_details_copy sd

JOIN public.uploadsdata_insertion_20260102030852 u
    ON sd.id = u.student_id
LEFT JOIN raw.course_mapping cmc
    ON LOWER(TRIM(u.currently_pursuing_degree)) = LOWER(TRIM(cmc.display_name))
                            
JOIN LATERAL unnest(
       string_to_array(u.subject_area, ',')
     ) AS subj(subject_name)
    ON TRUE
LEFT JOIN raw.subject_mapping sm
    ON LOWER(TRIM(subj.subject_name)) = LOWER(TRIM(sm.sub_field))
                            
LEFT JOIN raw.college_mapping cm
    ON LOWER(TRIM(u.college_name)) = LOWER(TRIM(cm.standard_college_names))
                            
LEFT JOIN raw.location_mapping lm
    ON LOWER(TRIM(u.district)) = LOWER(TRIM(lm.district))
    AND LOWER(TRIM(u.state_union_territory)) = LOWER(TRIM(lm.state_union_territory))
    AND LOWER(TRIM(u.city_category)) = LOWER(TRIM(lm.city_category))
                            
LEFT JOIN raw.university_mapping um
    ON LOWER(TRIM(u.university_name)) = LOWER(TRIM(um.standard_university_names))
GROUP BY
    sd.id,
    cmc.course_id,
    cm.college_id,
    um.university_id,
    lm.location_id,
    u.currently_pursuing_year,
    cmc.course_duration;
""")

try:
    with engine.begin() as conn:
        conn.execute(insert_students_sql)     #  insert students
        #update email id as per combination of student_id + vigyanshaala.com
        
        conn.execute(text("""                           
        UPDATE raw.student_details_copy
        SET email = id || '@vigyanshaala.com'
        WHERE email IS NULL
        """))
        

        #Run this after running the above two sql queries
        # Execute data insertion in other tables i.e student registration details and student education.

        #conn.execute(insert_registration_sql) #  insert registration
        #conn.execute(insert_education_sql)

except Exception as e:
    print(e)
