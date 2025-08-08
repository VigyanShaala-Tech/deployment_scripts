import sys
import os
from dotenv import load_dotenv
import psycopg
from psycopg import sql, errors

# Load credentials from config.env
load_dotenv("config.env")

PG_USER = os.getenv("DB_USER")
PG_PASSWORD = os.getenv("DB_PASSWORD")
PG_HOST = os.getenv("DB_HOST")
PG_PORT = os.getenv("DB_PORT")

def create_database_and_schema(db_name, schema_name):
    try:
        # Connect to the default 'postgres' database
        with psycopg.connect(
            dbname="postgres",
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            autocommit=True
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
                print(f"Database '{db_name}' created successfully.")
    except errors.DuplicateDatabase:
        print(f"Database '{db_name}' already exists. Skipping creation.")
    except Exception as e:
        print(f"Error creating database: {e}")
        sys.exit(1)

    try:
        # Connect to the new database to create the schema
        with psycopg.connect(
            dbname=db_name,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            autocommit=True
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
                print(f"Schema '{schema_name}' created in database '{db_name}'.")
    except Exception as e:
        print(f"Error creating schema: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python create_db_and_schema.py <database_name> <schema_name>")
        sys.exit(1)

    database_name = sys.argv[1]
    schema_name = sys.argv[2]
    create_database_and_schema(database_name, schema_name)