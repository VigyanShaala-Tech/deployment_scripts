import os
from sqlalchemy import text
import sys

from deployment_scripts.connection import get_engine, get_session, metadata

engine = get_engine()

# ENUM definitions
enum_definitions = {
    "gender_enum": ['F', 'M', 'O'],
    "program_code_enum": ['INC', 'ACC', 'STC'],
    "cohort_type_enum": ['open', 'curriculum'],
    "resource_category": ['Pre-recorded Video', 'Quiz', 'Assignment'],
    "submission_status_enum": ['under review', 'reviewed', 'rejected'],
    "session_type_enum": ['Masterclass', 'Speak Up Kalpana Session', 'Workshop','Extra Session']
}

target_schema = "raw"


def create_or_update_enums(schema, enum_definitions):

    try:
        with engine.begin() as conn:

            # Ensure schema exists
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
            print(f"✅ Schema '{schema}' ensured.\n")

            for enum_name, values in enum_definitions.items():

                # Check if ENUM exists
                check_query = text("""
                    SELECT 1
                    FROM pg_type t
                    JOIN pg_namespace n ON n.oid = t.typnamespace
                    WHERE t.typname = :enum_name AND n.nspname = :schema_name;
                """)

                exists = conn.execute(check_query, {
                    "enum_name": enum_name,
                    "schema_name": schema
                }).fetchone()

                if not exists:
                    # Create ENUM if missing
                    value_list = ", ".join(f"'{v}'" for v in values)
                    create_query = text(f"CREATE TYPE {schema}.{enum_name} AS ENUM ({value_list});")
                    conn.execute(create_query)
                    print(f"🆕 Created ENUM '{schema}.{enum_name}' with values: {values}\n")

                else:
                    print(f"⏩ ENUM '{schema}.{enum_name}' already exists. Checking for missing values...")

                    # Fetch current ENUM values
                    fetch_query = text("""
                        SELECT e.enumlabel
                        FROM pg_type t
                        JOIN pg_enum e ON t.oid = e.enumtypid
                        JOIN pg_namespace n ON n.oid = t.typnamespace
                        WHERE t.typname = :enum_name
                          AND n.nspname = :schema_name
                        ORDER BY e.enumsortorder;
                    """)

                    current_values = [row[0] for row in conn.execute(fetch_query, {
                        "enum_name": enum_name,
                        "schema_name": schema
                    }).fetchall()]

                    # Compare each expected value
                    for new_value in values:
                        if new_value not in current_values:

                            # Add missing ENUM value
                            add_query = text(f"""
                                ALTER TYPE {schema}.{enum_name}
                                ADD VALUE IF NOT EXISTS '{new_value}';
                            """)
                            conn.execute(add_query)
                            print(f"Added missing ENUM value '{new_value}'")

                    print("ENUM is now up to date.\n")

                    # Show final ENUM values
                    rows = conn.execute(fetch_query, {
                        "enum_name": enum_name,
                        "schema_name": schema
                    }).fetchall()

                    print(f"📌 Current ENUM values in '{schema}.{enum_name}':")
                    for r in rows:
                        print(" -", r[0])
                    print()

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Error during ENUM creation/update:", e)


# Run it
create_or_update_enums(target_schema, enum_definitions)