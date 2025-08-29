import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy import inspect

# Change the crendentials in the configuration.env file
def load_env(file_path):
    load_dotenv(file_path)
    return {
        'HOST' : os.getenv("DB_HOST"),
        'DB_NAME' : os.getenv("DB_NAME"),
        'USER' : os.getenv("DB_USER"),
        'PASSWORD': os.getenv("DB_PASSWORD"),
        'PORT' : os.getenv("DB_PORT")
        }

def loading_engine(config):
    return(create_engine(f"postgresql+psycopg2://{config['USER']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}/{config['DB_NAME']}"))

def import_csv_to_db(folder_path, engine, filter_text):
    files = os.listdir(folder_path)
    inspector = inspect(engine)

    csv_files = [
        f for f in files
        if f.endswith(".csv") and (filter_text in f if filter_text else True)
    ]

    if not csv_files:
        print("No matching CSV files found.")
        return
    
    for file in csv_files:
        if file.endswith(".csv"):
            file_path = os.path.join(folder_path, file)
            table_name = file.rsplit(".", 1)[0].replace(" ", "_").lower()
            schema = "raw"

            df = pd.read_csv(file_path, encoding="ISO-8859-1", sep=None, engine='python')

            if df.empty:
                print(f"Skipping empty file: {file}")
                continue

            with engine.begin() as conn:
                # Normalize columns for consistency
                df.columns = [col.lower() for col in df.columns]
                columns = df.columns

                # Ask user which schema to apply
                print(f"\nProcessing: {file}")
                print("Choose schema:")
                print("1 - user_id/data_fields/value")
                print("2 - email/session_code/duration/watched_on")
                print("3 - assignment_id/submitted_at/email")
                choice = input("Enter your choice (1 or 2 or 3): ").strip()

                constraints = inspector.get_unique_constraints(table_name, schema=schema)
                constraint_names = [c['name'] for c in constraints]

                if choice == "1":
                    if 'user_id' not in columns or 'data_fields' not in columns:
                        print(f"** Skipping {file}: Missing required columns for choice 1.")
                        return

                    constraint_name = f"{table_name}_user_data_key"
                    if constraint_name not in constraint_names:
                        try:
                            add_constraint_stmt = f"""
                                ALTER TABLE {schema}.{table_name}
                                ADD CONSTRAINT {constraint_name} UNIQUE (user_id, data_fields)
                            """
                            conn.execute(text(add_constraint_stmt))
                            print(f"Added UNIQUE constraint on (user_id, data_fields) to raw.{table_name}")
                        except Exception as e:
                            print(f"Warning: Could not add constraint. It might already exist. {e}")

                    for _, row in df.iterrows():
                        stmt = text(f"""
                            INSERT INTO {schema}.{table_name} (user_id, data_fields, value)
                            VALUES (:user_id, :data_fields, :value)
                            ON CONFLICT (user_id, data_fields)
                            DO UPDATE SET value = EXCLUDED.value
                        """)
                        conn.execute(stmt, {
                            "user_id": row['user_id'],
                            "data_fields": row['data_fields'],
                            "value": row['value']
                        })
                    print("Data upserted successfully")

                


                elif choice == "2":
                    df = pd.read_csv(file_path, encoding="ISO-8859-1", sep=None, engine='python')
                    df.columns = df.columns.str.strip().str.replace('\ufeff', '')  # Normalize headers

                    if df.empty:
                        print(f"** Skipping empty file: {file}")
                        continue

                    required_cols = ["Email", "Session_Code", "Duration_in_hrs", "Duration_in_secs", "watched_on"]
                    if not all(col in df.columns for col in required_cols):
                        print(f"** Skipping {file}: Missing required columns.")
                        continue

                    # Replace 'NaN', nan, or empty with None before upsert
                    df['watched_on'] = df['watched_on'].replace(['NaN', 'nan', '', float('nan')], None)
                    df['Duration_in_hrs'] = df['Duration_in_hrs'].replace(['NaN', 'nan', '', float('nan')], None)
                    df['Duration_in_secs'] = df['Duration_in_secs'].replace(['NaN', 'nan', '', float('nan')], None)

                    with engine.begin() as conn:

                        # Add unique constraint if not already present
                        constraints = inspector.get_unique_constraints(table_name, schema=schema)
                        constraint_names = [c['name'] for c in constraints]
                        constraint_name = f"{table_name}_user_data_key"
                        if constraint_name not in constraint_names:
                            try:
                                add_constraint_stmt = f"""
                                    ALTER TABLE {schema}."{table_name}"
                                    ADD CONSTRAINT {constraint_name} UNIQUE ("Email", "Session_Code")
                                """
                                conn.execute(text(add_constraint_stmt))
                                print(f"Added UNIQUE constraint on (Email, Session_Code) to raw.{table_name}")
                            except Exception as e:
                                print(f"** Warning: Could not add constraint. It might already exist. {e}")

                        # Check and add 'watched_on' column if missing
                        columns_in_db = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
                        if "watched_on" not in columns_in_db:
                            try:
                                alter_column_stmt = f"""
                                    ALTER TABLE {schema}."{table_name}"
                                    ADD COLUMN "watched_on" TEXT
                                """
                                conn.execute(text(alter_column_stmt))
                                print(f"## Added column 'watched_on' to raw.{table_name}")
                            except Exception as e:
                                print(f"** Warning: Could not add 'watched_on' column. It might already exist or failed: {e}")

                        # Perform upsert
                        for _, row in df.iterrows():
                            watched_on_value = row['watched_on'] if pd.notna(row['watched_on']) else None

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
                            conn.execute(stmt, {
                                "Email": row['Email'],
                                "Session_Code": row['Session_Code'],
                                "Duration_in_hrs": row['Duration_in_hrs'],
                                "Duration_in_secs": row['Duration_in_secs'],
                                "watched_on": watched_on_value
                            })
                        print("Data upserted successfully")


                elif choice == "3":
                    df = pd.read_csv(file_path, encoding="ISO-8859-1", sep=None, engine='python')
                    df.columns = df.columns.str.strip().str.replace('\ufeff', '')  # Normalize headers

                    if df.empty:
                        print(f"** Skipping empty file: {file}")
                        continue

                    required_cols = ["assignment_id", "submitted_at", "Email"]
                    if not all(col in df.columns for col in required_cols):
                        print(f"** Skipping {file}: Missing required columns.")
                        continue

                    with engine.begin() as conn:

                        # Add unique constraint if not already present
                        constraints = inspector.get_unique_constraints(table_name, schema=schema)
                        constraint_names = [c['name'] for c in constraints]
                        constraint_name = f"{table_name}_user_data_key"
                        if constraint_name not in constraint_names:
                            try:
                                add_constraint_stmt = f"""
                                    ALTER TABLE {schema}."{table_name}"
                                    ADD CONSTRAINT {constraint_name} UNIQUE ("assignment_id", "submitted_at", "Email")
                                """
                                conn.execute(text(add_constraint_stmt))
                                print(f" Added UNIQUE constraint on (assignment_id, submitted_at, Email) to raw.{table_name}")
                            except Exception as e:
                                print(f"** Warning: Could not add constraint. It might already exist. {e}")

                        all_columns = df.columns.tolist()
                        insert_cols = ", ".join(f'"{col}"' for col in all_columns)
                        insert_vals = ", ".join(f":{col}" for col in all_columns)

                        update_cols = [col for col in all_columns if col not in ["assignment_id", "submitted_at", "Email"]]
                        update_stmt = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in update_cols)

                        # Perform upsert
                        for _, row in df.iterrows():
                            stmt = text(f"""
                                INSERT INTO {schema}."{table_name}" ({insert_cols})
                                VALUES ({insert_vals})
                                ON CONFLICT ("assignment_id", "submitted_at", "Email")
                                DO UPDATE SET {update_stmt}
                            """)
                            conn.execute(stmt, {col: row[col] for col in all_columns})
                        print("Data upserted successfully")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 load_monitoring_csvs_to_db.py <folder_path>")
        sys.exit(1)

    folder_path = sys.argv[1]

    filter_text = input("Enter filename prefix or suffix to filter (leave blank to process all): ").strip()
    
    config = load_env("config.env")
    engine = loading_engine(config)
    import_csv_to_db(folder_path, engine, filter_text)