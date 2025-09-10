import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config.env")

# Get DB credentials
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database_name = os.getenv("DB_NAME")

# DB connection
connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(connection_string)

# IDs to delete
student_ids_to_delete = [1390, 9801, 9802, 9805, 9795, 9799, 9800, 9803, 9810]

# Tables to clean
tables_with_student_id = [
    "intermediate.referral_college_professor",
    "intermediate.student_education",
    "intermediate.student_registration_details"
]

student_details_table = "intermediate.student_details"        # key = id
raw_general_info_table = '"raw".general_information_sheet'    # key = "Student_id" (case-sensitive!)

# Backup file
backup_file = "deleted_students_backup.xlsx"
backup_data = {}

# Step 0: Update emails before deletion
with engine.begin() as conn:
    # Update in intermediate.student_details (column: email)
    conn.execute(
        text(f"UPDATE {student_details_table} SET email = :new_email WHERE id = :id"),
        {"new_email": "inc5_markalavaishnavi@gmail.com", "id": 3916}
    )
    conn.execute(
        text(f"UPDATE {student_details_table} SET email = :new_email WHERE id = :id"),
        {"new_email": "isadika@gmail.com", "id": 9117}
    )
    print("✅ Updated emails in intermediate.student_details")

    # Update in raw.general_information_sheet (column: \"Email\")
    conn.execute(
        text(f'UPDATE {raw_general_info_table} SET "Email" = :new_email WHERE "Student_id" = :id'),
        {"new_email": "inc5_markalavaishnavi@gmail.com", "id": 3916}
    )
    conn.execute(
        text(f'UPDATE {raw_general_info_table} SET "Email" = :new_email WHERE "Student_id" = :id'),
        {"new_email": "isadika@gmail.com", "id": 9117}
    )
    print("✅ Updated emails in raw.general_information_sheet")

# Step 1: Backup rows
with engine.connect() as conn:
    # student_details backup
    backup_data["student_details"] = pd.read_sql(
        text(f"SELECT * FROM {student_details_table} WHERE id = ANY(:ids)"),
        conn,
        params={"ids": student_ids_to_delete}
    )

    # intermediate child tables
    for table in tables_with_student_id:
        backup_data[table] = pd.read_sql(
            text(f"SELECT * FROM {table} WHERE student_id = ANY(:ids)"),
            conn,
            params={"ids": student_ids_to_delete}
        )

    # raw.general_information_sheet backup
    backup_data["raw.general_information_sheet"] = pd.read_sql(
        text(f'SELECT * FROM {raw_general_info_table} WHERE "Student_id" = ANY(:ids)'),
        conn,
        params={"ids": student_ids_to_delete}
    )

# Save backups into one Excel
with pd.ExcelWriter(backup_file) as writer:
    for table, df in backup_data.items():
        sheet_name = table.replace(".", "_")
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"📝 Backup saved to: {backup_file}")

# Step 2: Perform deletion (child → parent)
with engine.begin() as conn:
    # Delete from intermediate child tables
    for table in tables_with_student_id:
        delete_query = text(f"DELETE FROM {table} WHERE student_id = ANY(:ids)")
        conn.execute(delete_query, {"ids": student_ids_to_delete})
        print(f"✅ Deleted from {table}")

    # Delete from student_details
    conn.execute(
        text(f"DELETE FROM {student_details_table} WHERE id = ANY(:ids)"),
        {"ids": student_ids_to_delete}
    )
    print(f"✅ Deleted from {student_details_table}")

    # Delete from raw.general_information_sheet
    conn.execute(
        text(f'DELETE FROM {raw_general_info_table} WHERE "Student_id" = ANY(:ids)'),
        {"ids": student_ids_to_delete}
    )
    print(f"✅ Deleted from {raw_general_info_table}")

print("🎯 Cleanup completed successfully!")
