import os
from sqlalchemy import text
import sys

from deployment_scripts.connection import get_engine, get_session, metadata

# Connect to PostgreSQL
engine = get_engine()

# ENUM definitions
enum_definitions = {
    "gender_enum": ['F', 'M', 'O'],
    "program_code_enum": ['INC', 'ACC', 'STC'],
    "cohort_type_enum": ['open', 'curriculum'],
    "resource_category": ['Pre-recorded Video', 'Quiz', 'Assignment'],
    "submission_status_enum": ['under review', 'reviewed', 'rejected'],
    "session_type_enum": ['Masterclass', 'Speak Up Kalpana Session', 'Workshop']

}

target_schema = "intermediate"

def create_enums(schema, enum_definitions):
    try:
        with engine.begin() as conn:
            # Ensure schema exists
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
            print(f"✅ Schema '{schema}' ensured.")

            for enum_name, values in enum_definitions.items():
                # Check if ENUM exists
                check_query = text("""
                    SELECT 1 FROM pg_type t
                    JOIN pg_namespace n ON n.oid = t.typnamespace
                    WHERE t.typname = :enum_name AND n.nspname = :schema_name;
                """)
                result = conn.execute(check_query, {
                    "enum_name": enum_name,
                    "schema_name": schema
                }).fetchone()

                if result:
                    print(f"⏩ ENUM '{schema}.{enum_name}' already exists.")
                else:
                    value_list = ", ".join(f"'{v}'" for v in values)
                    create_query = text(f"CREATE TYPE {schema}.{enum_name} AS ENUM ({value_list});")
                    conn.execute(create_query)
                    print(f"✅ Created ENUM '{schema}.{enum_name}' with values: {values}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Error during ENUM creation:", e)

# Run the ENUM creation
create_enums(target_schema, enum_definitions)
