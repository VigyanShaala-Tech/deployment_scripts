import os
from sqlalchemy import text
from deployment_scripts.connection import get_engine
import pandas as pd
from datetime import datetime
import sys

engine = get_engine()

# --------------------------------
# Define timestamps
# --------------------------------

education_start_time = "2026-03-05 19:46:00"
education_end_time = "2026-03-05 19:47:00"

registration_start_time = "2026-03-05 21:22:00"
registration_end_time = "2026-03-05 21:23:00"


# --------------------------------
# Preview queries
# --------------------------------

education_preview_query = text("""
SELECT *
FROM raw.student_education
WHERE inserted_at >= :start_time
AND inserted_at < :end_time
""")

registration_preview_query = text("""
SELECT *
FROM raw.student_registration_details
WHERE inserted_at >= :start_time
AND inserted_at < :end_time
""")


# --------------------------------
# Delete queries
# --------------------------------

delete_education_query = text("""
DELETE FROM raw.student_education
WHERE inserted_at >= :start_time
AND inserted_at < :end_time
""")

delete_registration_query = text("""
DELETE FROM raw.student_registration_details
WHERE inserted_at >= :start_time
AND inserted_at < :end_time
""")


# --------------------------------
# Preview rows
# --------------------------------

education_df = pd.read_sql(
    education_preview_query,
    engine,
    params={"start_time": education_start_time, "end_time": education_end_time}
)

registration_df = pd.read_sql(
    registration_preview_query,
    engine,
    params={"start_time": registration_start_time, "end_time": registration_end_time}
)

print(f"\nEducation rows to delete: {len(education_df)}")
print(f"Registration rows to delete: {len(registration_df)}")

if len(education_df) == 0 and len(registration_df) == 0:
    print("No rows found. Exiting safely.")
    sys.exit()


# --------------------------------
# Save logs to CSV
# --------------------------------

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

education_log = f"education_delete_log_{timestamp}.csv"
registration_log = f"registration_delete_log_{timestamp}.csv"

education_df.to_csv(education_log, index=False)
registration_df.to_csv(registration_log, index=False)

print("\nLog files created:")
print(education_log)
print(registration_log)


# --------------------------------
# Show sample preview
# --------------------------------

print("\nSample education rows:")
print(education_df.head())

print("\nSample registration rows:")
print(registration_df.head())


# --------------------------------
# Confirmation step
# --------------------------------

confirm = input("\nType 'yes' to proceed with deletion: ")

if confirm.lower() != "yes":
    print("Deletion cancelled.")
    sys.exit()


# --------------------------------
# Execute delete queries
# --------------------------------

try:
    with engine.begin() as conn:
        result = conn.execute(
            delete_education_query,
            {"start_time": education_start_time, "end_time": education_end_time}
        )
        print(f"\nStudent education rows deleted: {result.rowcount}")

except Exception as e:
    print("\nError deleting student education rows:")
    print(e)


try:
    with engine.begin() as conn:
        result = conn.execute(
            delete_registration_query,
            {"start_time": registration_start_time, "end_time": registration_end_time}
        )
        print(f"Student registration rows deleted: {result.rowcount}")

except Exception as e:
    print("\nError deleting student registration rows:")
    print(e)


print("\nScript execution finished.")