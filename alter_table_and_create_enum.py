import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config.env")

# Database credentials
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME")

# Connect to PostgreSQL
engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}",
    echo=False
)

target_schema = "intermediate"

# ENUM definitions
enum_definitions = {
    "gender_enum": ['F', 'M', 'O'],
    "program_code_enum": ['INC', 'ACC', 'STC'],
    "cohort_type_enum": ['open', 'curriculum'],
    "resource_category": ['Pre-recorded Video', 'Quiz', 'Assignment'],
    "submission_status_enum": ['under review', 'reviewed', 'rejected', 'submitted'],
    "session_type_enum": ['Masterclass', 'Speak Up Kalpana Session', 'Workshop']
}

# ALTER TABLE statements to add primary keys
alter_statements = [
    f"ALTER TABLE {target_schema}.college_mapping ADD CONSTRAINT college_mapping_pkey PRIMARY KEY (college_id);",
    f"ALTER TABLE {target_schema}.subject_mapping ADD CONSTRAINT subject_mapping_pkey PRIMARY KEY (id);",
    f"ALTER TABLE {target_schema}.course_mapping ADD CONSTRAINT course_mapping_pkey PRIMARY KEY (course_id);",
    f"ALTER TABLE {target_schema}.location_mapping ADD CONSTRAINT location_mapping_pkey PRIMARY KEY (location_id);",
    f"ALTER TABLE {target_schema}.university_mapping ADD CONSTRAINT university_mapping_pkey PRIMARY KEY (university_id);"
]


def create_enums_and_alter_tables(schema, enum_definitions):
    try:
        with engine.begin() as conn:
            # Ensure schema exists
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
            print(f"✅ Schema '{schema}' ensured.")

            # ENUM creation
            for enum_name, values in enum_definitions.items():
                check_enum_query = text("""
                    SELECT 1 FROM pg_type t
                    JOIN pg_namespace n ON n.oid = t.typnamespace
                    WHERE t.typname = :enum_name AND n.nspname = :schema_name;
                """)
                result = conn.execute(check_enum_query, {
                    "enum_name": enum_name,
                    "schema_name": schema
                }).fetchone()

                if result:
                    print(f"⏩ ENUM '{schema}.{enum_name}' already exists.")
                else:
                    value_list = ", ".join(f"'{v}'" for v in values)
                    create_enum_query = text(f"CREATE TYPE {schema}.{enum_name} AS ENUM ({value_list});")
                    conn.execute(create_enum_query)
                    print(f"✅ Created ENUM '{schema}.{enum_name}' with values: {values}")

            # primary key addition
            for statement in alter_statements:
                try:
                    # Extract table name
                    tokens = statement.split()
                    full_table = tokens[2]
                    schema_name, table_name = full_table.split(".")

                    # Check if PK already exists on the table
                    pk_check_query = text("""
                        SELECT 1
                        FROM information_schema.table_constraints
                        WHERE table_schema = :schema
                          AND table_name = :table
                          AND constraint_type = 'PRIMARY KEY';
                    """)
                    result = conn.execute(pk_check_query, {
                        "schema": schema_name,
                        "table": table_name
                    }).fetchone()

                    if result:
                        print(f"⏩ Primary key already exists on '{schema_name}.{table_name}', skipping.")
                    else:
                        conn.execute(text(statement))
                        print(f"✅ Executed: {statement.strip()}")

                except Exception as e:
                    print(f"❌ Error executing: {statement.strip()} --> {e}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Error during ENUM or PK creation:", e)

# Execute all steps
create_enums_and_alter_tables(target_schema, enum_definitions)
