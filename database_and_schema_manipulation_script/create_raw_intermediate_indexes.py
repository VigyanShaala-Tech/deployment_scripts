import os
import sys
from sqlalchemy import text

from deployment_scripts.connection import get_engine, get_session, metadata

engine = get_engine()

# Student Assignment (dedupe on student_id, resource_id, submitted_at)
DUP_CHECK_ASSIGNMENT = text("""
    SELECT student_id, resource_id, submitted_at, COUNT(*) AS cnt
    FROM intermediate.student_assignment
    GROUP BY student_id, resource_id, submitted_at
    HAVING COUNT(*) > 1;
""")

DEDUP_ASSIGNMENT = text("""
    DELETE FROM intermediate.student_assignment sq
    USING (
        SELECT ctid,
               ROW_NUMBER() OVER (
                   PARTITION BY student_id, resource_id, submitted_at
                   ORDER BY ctid
               ) AS rn
        FROM intermediate.student_assignment
    ) sub
    WHERE sq.ctid = sub.ctid
      AND sub.rn > 1;
""")

# Student Quiz (dedupe on student_id, resource_id)
DUP_CHECK_QUIZ = text("""
    SELECT student_id, resource_id, COUNT(*) AS cnt
    FROM intermediate.student_quiz
    GROUP BY student_id, resource_id
    HAVING COUNT(*) > 1;
""")

DEDUP_QUIZ = text("""
    DELETE FROM intermediate.student_quiz sq
    USING (
        SELECT ctid,
               ROW_NUMBER() OVER (
                   PARTITION BY student_id, resource_id
                   ORDER BY ctid
               ) AS rn
        FROM intermediate.student_quiz
    ) sub
    WHERE sq.ctid = sub.ctid
      AND sub.rn > 1;
""")

DUP_CHECK_SESSION = text("""
    SELECT student_id, session_id, COUNT(*) AS cnt
    FROM intermediate.student_session
    GROUP BY student_id, session_id
    HAVING COUNT(*) > 1;
""")

DEDUP_SESSION = text("""
    DELETE FROM intermediate.student_session sq
    USING (
        SELECT ctid,
               ROW_NUMBER() OVER (
                   PARTITION BY student_id, session_id
                   ORDER BY ctid
               ) AS rn
        FROM intermediate.student_session
    ) sub
    WHERE sq.ctid = sub.ctid
      AND sub.rn > 1;
""")

RAW_TABLES = {
    "incubator_quiz_monitoring": ("user_id", "data_fields"),
    "student_session_information": ("Email", "Session_Code"),
    "assignment_monitoring_data": ("assignment_id", "submitted_at", "Email"),
}

def generate_raw_dedup_queries():
    dup_checks = {}
    dedups = {}

    for table, cols in RAW_TABLES.items():
        col_list = ", ".join(f'"{c}"' for c in cols)
        join_conditions = " AND ".join(f'"{c}" = t."{c}"' for c in cols)

        dup_checks[table] = text(f"""
            SELECT {col_list}, COUNT(*) AS cnt
            FROM raw.{table}
            GROUP BY {col_list}
            HAVING COUNT(*) > 1;
        """)

        dedups[table] = text(f"""
            DELETE FROM raw.{table} a
            USING (
                SELECT MIN(ctid) AS keep_ctid, {col_list}
                FROM raw.{table}
                GROUP BY {col_list}
                HAVING COUNT(*) > 1
            ) t
            WHERE {join_conditions}
              AND a.ctid <> t.keep_ctid;
        """)

    return dup_checks, dedups


RAW_DUP_CHECKS, RAW_DEDUPS = generate_raw_dedup_queries()


CREATE_INDEXES = [
    text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_student_assignment_unique
        ON intermediate.student_assignment(student_id, resource_id, submitted_at);
    """),
    text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_student_session_unique
        ON intermediate.student_session(student_id, session_id);
    """),
    text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_student_quiz_unique
        ON intermediate.student_quiz(student_id, resource_id);
    """),
]


def main():
    try:
        with engine.begin() as conn:
            # Assignment: check + dedupe
            dup_rows = conn.execute(DUP_CHECK_ASSIGNMENT).fetchall()
            if dup_rows:
                print("** Found duplicates in intermediate.student_assignment:")
                for r in dup_rows:
                    print(f"  (student_id={r.student_id}, resource_id={r.resource_id}, submitted_at={r.submitted_at}) -> {r.cnt} rows")
                deleted = conn.execute(DEDUP_ASSIGNMENT).rowcount
                print(f"** Removed {deleted} duplicate row(s) from intermediate.student_assignment.")
                if conn.execute(DUP_CHECK_ASSIGNMENT).fetchall():
                    raise RuntimeError("Duplicates remain in student_assignment after dedup. Abort indexing.")
            else:
                print("* No duplicates in intermediate.student_assignment.")

            # Quiz: check + dedupe
            quiz_dups = conn.execute(DUP_CHECK_QUIZ).fetchall()
            if quiz_dups:
                print("** Found duplicates in intermediate.student_quiz:")
                for r in quiz_dups:
                    print(f"  (student_id={r.student_id}, resource_id={r.resource_id}) -> {r.cnt} rows")
                q_deleted = conn.execute(DEDUP_QUIZ).rowcount
                print(f"** Removed {q_deleted} duplicate row(s) from intermediate.student_quiz.")
                if conn.execute(DUP_CHECK_QUIZ).fetchall():
                    raise RuntimeError("Duplicates remain in student_quiz after dedup. Abort indexing.")
            else:
                print("* No duplicates in intermediate.student_quiz.")

            session_dups = conn.execute(DUP_CHECK_SESSION).fetchall()
            if session_dups:
                print("** Found duplicates in intermediate.student_session:")
                for r in session_dups:
                    print(f"  (student_id={r.student_id}, session_id={r.session_id}) -> {r.cnt} rows")
                s_deleted = conn.execute(DEDUP_SESSION).rowcount
                print(f"** Removed {s_deleted} duplicate row(s) from intermediate.student_session.")
                if conn.execute(DUP_CHECK_SESSION).fetchall():
                    raise RuntimeError("Duplicates remain in student_session after dedup. Abort indexing.")
            else:
                print("* No duplicates in intermediate.student_session.")

            
            for table, dup_query in RAW_DUP_CHECKS.items():
                raw_dups = conn.execute(dup_query).fetchall()
                if raw_dups:
                    print(f"** Found duplicates in raw.{table}:")
                    for r in raw_dups:
                        print(f"  ({', '.join(f'{col}={getattr(r, col)}' for col in r._fields if col != 'cnt')}) -> {r.cnt} rows")
                    deleted = conn.execute(RAW_DEDUPS[table]).rowcount
                    print(f"** Removed {deleted} duplicate row(s) from raw.{table}.")
                    if conn.execute(dup_query).fetchall():
                        raise RuntimeError(f"Duplicates remain in raw.{table} after dedup. Abort indexing.")
                else:
                    print(f"* No duplicates in raw.{table}.")

            # Create/ensure unique indexes (after cleaning)
            for stmt in CREATE_INDEXES:
                conn.execute(stmt)
            print("** Unique indexes ensured (created if missing).")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(" * Error during cleanup/indexing:", e)

if __name__ == "__main__":
    main()
