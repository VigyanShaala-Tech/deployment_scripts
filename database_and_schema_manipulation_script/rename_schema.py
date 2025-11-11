import os
import logging
from sqlalchemy import create_engine

import psycopg2
from psycopg2 import sql, errors
from deployment_scripts.connection import get_engine, get_session, metadata
 
# -------------------------------
# Set Up Logging
# -------------------------------
logging.basicConfig(
    filename='/home/ubuntu/oct9/deployment_scripts/database_and_schema_manipulation_script/schema_rename.log',
    level=logging.DEBUG,  # Log all messages (DEBUG, INFO, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s'
)

 
# -------------------------------
# Schema rename mapping
# -------------------------------
RENAMES = [
    ("raw", "old"),
    ("intermediate", "raw"),
    ("final", "intermediate")
]
 
# -------------------------------
# Function to check if a schema exists
# -------------------------------
def schema_exists(cursor, schema_name):
    cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;", (schema_name,))
    return cursor.fetchone() is not None
 
# -------------------------------
# Function to check the connection
# -------------------------------
def check_connection(raw_conn):
    try:
        # Query the connection to check user details
        with raw_conn.cursor() as cur:
            cur.execute("""
                SELECT current_user, session_user, 
                       (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) AS is_superuser,
                       inet_server_addr(), inet_server_port(), pg_backend_pid();
            """)
            user_info = cur.fetchall()
 
            logging.info(f"Connected as: {user_info[0][0]}")
            logging.info(f"Session user: {user_info[0][1]}")
            logging.info(f"Superuser privileges: {user_info[0][2]}")
            logging.info(f"Server address: {user_info[0][3]}")
            logging.info(f"Server port: {user_info[0][4]}")
            logging.info(f"PostgreSQL backend process ID: {user_info[0][5]}")


        engine = get_engine()
        host = engine.url.host or "localhost"
        server_ip = user_info[3]

        if host in ("localhost", "127.0.0.1"):
            logging.info("Connection request is targeting local host.")
            if server_ip == "127.0.0.1":
                logging.info("Server confirms TCP loopback connection.")
            else:
                logging.info("Likely using Unix socket connection.")
        else:
            logging.info(f"Connection is TCP/IP to remote host: {host}")

    except Exception as e:
        logging.error(f"Connection info error: {e}")        
 
# -------------------------------
# Function to rename schemas
# -------------------------------
def rename_schemas():
    engine = get_engine()
 
    # Use raw DBAPI connection to enable autocommit for DDL
    raw_conn = engine.raw_connection()
    raw_conn.autocommit = True
    cur = raw_conn.cursor()
 
    check_connection(raw_conn)
    try:
        for old_schema, new_schema in RENAMES:
            # Log before renaming the schema
            logging.info(f"Attempting to rename schema '{old_schema}' to '{new_schema}'")
 
            # Check if the new schema already exists
            if schema_exists(cur, new_schema):
                logging.warning(f"Schema '{new_schema}' already exists. Skipping rename operation for '{old_schema}' to '{new_schema}'.")
                continue
 
            # Execute schema rename
            try:
                cur.execute(f'ALTER SCHEMA "{old_schema}" RENAME TO "{new_schema}";')
                logging.info(f"Renamed schema '{old_schema}' to '{new_schema}' successfully.")
            except Exception as rename_error:
                logging.error(f"Failed to rename schema '{old_schema}' to '{new_schema}': {rename_error}")
        raw_conn.commit()  # Explicit commit
 
        logging.info("Schema rename process completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred during schema renaming process: {e}")
 
    finally:
        cur.close()
        raw_conn.close()
 
if __name__ == "__main__":
    logging.info("Starting schema rename process...")
    rename_schemas()