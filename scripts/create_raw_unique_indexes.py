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

# Unique index definitions
unique_indexes = [
    {
        "index_name": "idx_assignment_monitoring_unique",
        "schema": "raw",
        "table": "assignment_monitoring_data",
        "columns": ["assignment_id", "submitted_at", "Email"]
    },
    {
        "index_name": "idx_quiz_monitoring_unique",
        "schema": "raw",
        "table": "incubator_quiz_monitoring",
        "columns": ["user_id", "data_fields"]
    },
    {
        "index_name": "idx_student_session_unique",
        "schema": "raw",
        "table": "student_session_information",
        "columns": ["Email", "Session_Code"]
    }
]


def create_unique_indexes(index_definitions):
    try:
        with engine.begin() as conn:
            for idx in index_definitions:
                schema = idx["schema"]
                table = idx["table"]
                index_name = idx["index_name"]
                columns = idx["columns"]

                # Check if index exists
                index_check_query = text("""
                    SELECT 1
                    FROM pg_indexes
                    WHERE schemaname = :schema
                      AND tablename = :table
                      AND indexname = :index_name;
                """)
                result = conn.execute(index_check_query, {
                    "schema": schema,
                    "table": table,
                    "index_name": index_name
                }).fetchone()

                if result:
                    print(f"⏩ Index '{schema}.{index_name}' already exists.")
                else:
                    column_list = ", ".join(f'"{col}"' for col in columns)
                    create_index_query = text(
                        f'CREATE UNIQUE INDEX {index_name} ON "{schema}".{table} ({column_list});'
                    )
                    conn.execute(create_index_query)
                    print(f"✅ Created unique index '{schema}.{index_name}' on columns: {columns}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("❌ Error during index creation:", e)


# Execute the function
create_unique_indexes(unique_indexes)
