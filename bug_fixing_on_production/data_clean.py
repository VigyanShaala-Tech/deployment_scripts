import os
from sqlalchemy import create_engine, text
from deployment_scripts.connection import get_engine, get_session, metadata

# Connect to PostgreSQL
engine = get_engine()

def clean_student_details():
    try:
        with engine.begin() as conn:
            # ---- 1. Fix caste column ----
            conn.execute(text("""
                UPDATE intermediate.student_details
                SET caste = 'Other'
                WHERE LOWER(TRIM(caste)) = 'others';
            """))
            print("✅ Updated caste values")

            # ---- 2. Fix annual_family_income column ----
            # Below or equal to 3 lacs
            conn.execute(text("""
                UPDATE intermediate.student_details
                SET annual_family_income_inr = 'Below or Equal to 3 lacs per year (INR)'
                WHERE TRIM(annual_family_income_inr) ILIKE 'Below 3 Lacs'
                   OR TRIM(annual_family_income_inr) ILIKE 'Less than 3 Lacs'
                   OR TRIM(annual_family_income_inr) ILIKE 'Below or Equal to 3 Lacs per year (INR)'                              ;
            """))

            # Above 5 lacs (including 12 lacs case)
            conn.execute(text("""
                UPDATE intermediate.student_details
                SET annual_family_income_inr = 'Above 5 lacs per year (INR)'
                WHERE TRIM(annual_family_income_inr) ILIKE 'Above 5 lacs'
                   OR TRIM(annual_family_income_inr) ILIKE 'Above 12 lacs'                          
                   OR TRIM(annual_family_income_inr) ILIKE 'Below 12 lacs';
            """))

            # Between 3–5 lacs
            conn.execute(text("""
                UPDATE intermediate.student_details
                SET annual_family_income_inr = 'Between 3-5 lacs (INR) per year'
                WHERE TRIM(annual_family_income_inr) ILIKE 'Below 5 Lacs'
                   OR TRIM(annual_family_income_inr) ILIKE 'Above 3 lacs per year (INR)'
                   OR TRIM(annual_family_income_inr) ILIKE 'Less than 5 lacs';
            """))

            print("✅ Updated annual_family_income values")

            # ---- 3. Fix wrong college_id in student_education ----
            conn.execute(text("""
                UPDATE intermediate.student_education
                SET college_id = 243
                WHERE college_id = 233;
            """))
            print("✅ Updated college_id from 233 → 243 in student_education")


            # ---- 4. Fix currently_pursuing_year inside JSONB (form_details) ----
            # Trim spaces
            conn.execute(text("""
                UPDATE intermediate.student_registration_details_2
                SET form_details = jsonb_set(
                    form_details,
                    '{currently_pursuing_year}',
                    to_jsonb(TRIM(BOTH FROM form_details->>'currently_pursuing_year'))
                )
                WHERE form_details ? 'currently_pursuing_year';
            """))

            # Replace Persued -> Pursued
            conn.execute(text("""
            UPDATE intermediate.student_registration_details_2
            SET form_details = jsonb_set(
                form_details,
                '{currently_pursuing_year}',
                to_jsonb('Pursued'::text)   
            )
            WHERE form_details->>'currently_pursuing_year' ILIKE 'Persued';
            """))

            print("✅ Cleaned currently_pursuing_year in form_details JSONB")


            # ---- 5. Fix standard_college_names in college_mapping ----
            conn.execute(text("""
                UPDATE intermediate.college_mapping
                SET standard_college_names = 'Narsee Monjee Institute of Management Studies (NMIMS)'
                WHERE standard_college_names = 'ACS Medical College & Hospital';
            """))
            print("✅ Updated standard_college_names from 'ACS Medical College & Hospital' → 'Narsee Monjee Institute of Management Studies (NMIMS)'")



    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Error during cleanup:", e)


# Run cleanup
clean_student_details()
