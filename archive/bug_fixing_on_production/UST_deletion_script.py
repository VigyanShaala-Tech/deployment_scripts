import csv
from datetime import datetime
from sqlalchemy import text
from deployment_scripts.connection import get_engine   # ? Your custom connection
engine = get_engine()


# ---------------------------------------------
# Hardcoded SELECT + DELETE pairs
# ---------------------------------------------
QUERY = [

    # 1) raw.student_assignment
    (
        """SELECT * FROM intermediate.student_assignment
           WHERE submitted_at::text ~ '\\.[0-9]+'
             AND cohort_code IN ('INC007', 'INC008', 'INC009');""",

        """DELETE FROM intermediate.student_assignment
           WHERE submitted_at::text ~ '\\.[0-9]+'
             AND cohort_code IN ('INC007', 'INC008', 'INC009');"""
    ),

    # 2) old.assignment_monitoring_data
    (
        """SELECT amd.assignment_name, amd."Email", amd.submission_status,
                     amd.submitted_at, gis."Incubator_Batch"
           FROM raw.assignment_monitoring_data AS amd
           LEFT JOIN raw.general_information_sheet AS gis
           ON amd."Email" = gis."Email"
           WHERE amd.submitted_at ~ 'T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z'
             AND gis."Incubator_Batch" IN ('Incubator 7.0','Incubator 8.0','Incubator 9.0');""",

        """DELETE FROM raw.assignment_monitoring_data
           WHERE submitted_at ~ 'T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z'
             AND "Email" IN (
                SELECT "Email" FROM raw.general_information_sheet
                WHERE "Incubator_Batch" IN ('Incubator 7.0','Incubator 8.0','Incubator 9.0')
           );"""
    ),

    # 3) intermediate.final_assignment
    (
        """SELECT * FROM final.final_assignment
           WHERE submitted_at::text ~ '\\.[0-9]+'
            AND cohort_code IN ('INC007', 'INC008', 'INC009');""",

        """DELETE FROM final.final_assignment
           WHERE submitted_at::text ~ '\\.[0-9]+'
            AND cohort_code IN ('INC007','INC008','INC009');"""
    )
]



# ---------------------------------------------
# Save CSV backup before deletion
# ---------------------------------------------
def export_to_csv(rows, columns, prefix):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"CSV backup created: {filename}")


# ---------------------------------------------
# Main Execution Loop
# ---------------------------------------------
with engine.begin() as conn:

    for idx, (select_sql, delete_sql) in enumerate(QUERY, start=1):

        print(f"\n--------------------------------------")
        print(f"Running QUERY #{idx}")
        print(f"--------------------------------------\n")

        # FETCH rows to be deleted
        result = conn.execute(text(select_sql))
        rows = result.fetchall()
        columns = result.keys()

        print(f"Rows matched for deletion: {len(rows)}")

        # Backup to CSV
        if rows:
            export_to_csv(rows, columns, prefix=f"delete_query_{idx}")
        else:
            print("No rows found for this query.")
            continue

        # Confirmation
        confirm = input(f"Do you want to DELETE these {len(rows)} rows? (yes/no): ").strip().lower()

        if confirm != "yes":
            print("DELETE skipped for this job.")
            continue

        # Execute delete
        conn.execute(text(delete_sql))
        print(f"DELETE executed for job #{idx}")

print("\n All delete operations completed.")
