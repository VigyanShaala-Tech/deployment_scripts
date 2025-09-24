import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy import inspect

# load env

def load_env(file_path):
    load_dotenv(file_path)
    return {
        'HOST': os.getenv("DB_HOST"),
        'DB_NAME': os.getenv("DB_NAME"),
        'USER': os.getenv("DB_USER"),
        'PASSWORD': os.getenv("DB_PASSWORD"),
        'PORT': os.getenv("DB_PORT")
    }

def loading_engine(config):
    return create_engine(
        f"postgresql+psycopg2://{config['USER']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}/{config['DB_NAME']}"
    )

# CSV import and upsert

def import_csv_to_db(folder_path, engine):                                              
    files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    inspector = inspect(engine)

    if not files:
        print("No CSV files found in the folder.")
        return

    for file in files:
        file_path = os.path.join(folder_path, file)
        table_name = file.rsplit(".", 1)[0].replace(" ", "_").lower()
        schema = "raw"

        print(f"\n* Processing: {file}")

        df = pd.read_csv(file_path, encoding="ISO-8859-1", sep=None, engine='python')
        if df.empty:
            print(f"## Skipping empty file: {file}")
            continue

        df.columns = df.columns.str.strip().str.replace('\ufeff', '')
        columns = [c.lower() for c in df.columns]
        df.columns = columns

        with engine.begin() as conn:
            try:
                # user_id/data_fields/value 

                if {"user_id", "data_fields", "value"}.issubset(columns):
                    constraint_name = f"{table_name}_user_data_key"
                    constraints = inspector.get_unique_constraints(table_name, schema=schema)
                    if constraint_name not in [c['name'] for c in constraints]:
                        try:
                            conn.execute(text(f"""
                                ALTER TABLE {schema}.{table_name}
                                ADD CONSTRAINT {constraint_name} UNIQUE (user_id, data_fields)
                            """))
                            print(f"** Added UNIQUE constraint (user_id, data_fields) on raw.{table_name}")
                        except Exception as e:
                            print(f"## Constraint warning: {e}")

                    for _, row in df.iterrows():
                        stmt = text(f"""
                            INSERT INTO {schema}.{table_name} (user_id, data_fields, value)
                            VALUES (:user_id, :data_fields, :value)
                            ON CONFLICT (user_id, data_fields)
                            DO UPDATE SET value = EXCLUDED.value
                        """)
                        conn.execute(stmt, dict(row))
                    print("** Data upserted successfully (user_id/data_fields/value)")

                # email/session_code/duration/watched_on 

                elif {"email", "session_code"}.issubset(columns):
                    constraint_name = f"{table_name}_email_session_key"
                    constraints = inspector.get_unique_constraints(table_name, schema=schema)
                    if constraint_name not in [c['name'] for c in constraints]:
                        try:
                            conn.execute(text(f"""
                                ALTER TABLE {schema}."{table_name}"
                                ADD CONSTRAINT {constraint_name} UNIQUE ("Email", "Session_Code")
                            """))
                            print(f"** Added UNIQUE constraint (Email, Session_Code) on raw.{table_name}")
                        except Exception as e:
                            print(f" Constraint warning: {e}")

                    # Ensure watched_on column exists

                    existing_cols = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
                    if "watched_on" not in existing_cols:
                        try:
                            conn.execute(text(f"""
                                ALTER TABLE {schema}."{table_name}"
                                ADD COLUMN "watched_on" TEXT
                            """))
                            print("** Added missing 'watched_on' column.")
                        except Exception as e:
                            print(f"# Column addition failed: {e}")

                    for _, row in df.iterrows():
                        stmt = text(f"""
                            INSERT INTO {schema}."{table_name}" 
                                ("Email", "Session_Code", "Duration_in_hrs", "Duration_in_secs", "watched_on")
                            VALUES 
                                (:Email, :Session_Code, :Duration_in_hrs, :Duration_in_secs, :watched_on)
                            ON CONFLICT ("Email", "Session_Code")                                
                            DO UPDATE SET
                                "Duration_in_hrs" = EXCLUDED."Duration_in_hrs",
                                "Duration_in_secs" = EXCLUDED."Duration_in_secs",
                                "watched_on" = EXCLUDED."watched_on"
                        """)
                        conn.execute(stmt, dict(row))
                    print("** Data upserted successfully (session watch data)")

                # assignment_id/submitted_at/email 
                
                elif {"assignment_id", "submitted_at", "email"}.issubset(columns):
                    constraint_name = f"{table_name}_assignment_key"
                    constraints = inspector.get_unique_constraints(table_name, schema=schema)
                    if constraint_name not in [c['name'] for c in constraints]:
                        try:
                            conn.execute(text(f"""
                                ALTER TABLE {schema}."{table_name}"
                                ADD CONSTRAINT {constraint_name} UNIQUE ("assignment_id", "submitted_at", "Email")
                            """))
                            print(f"** Added UNIQUE constraint (assignment_id, submitted_at, Email) on raw.{table_name}")
                        except Exception as e:
                            print(f"## Constraint warning: {e}")

                    all_columns = df.columns.tolist()
                    insert_cols = ", ".join(f'"{col}"' for col in all_columns)
                    insert_vals = ", ".join(f":{col}" for col in all_columns)

                    update_cols = [col for col in all_columns if col not in ["assignment_id", "submitted_at", "email"]]
                    update_stmt = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in update_cols)

                    for _, row in df.iterrows():
                        stmt = text(f"""
                            INSERT INTO {schema}."{table_name}" ({insert_cols})
                            VALUES ({insert_vals})
                            ON CONFLICT ("assignment_id", "submitted_at", "Email")
                            DO UPDATE SET {update_stmt}
                        """)
                        conn.execute(stmt, dict(row))
                    print("* Data upserted successfully (assignment submissions)")

                else:
                    print(f"##Skipped {file}: Could not auto-detect schema type based on columns.")

            except Exception as e:
                print(f"Error processing {file}: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 load_monitoring_csvs_to_db.py <folder_path>")
        sys.exit(1)

    folder_path = sys.argv[1]
    config = load_env("config.env")
    engine = loading_engine(config)

    import_csv_to_db(folder_path, engine)
