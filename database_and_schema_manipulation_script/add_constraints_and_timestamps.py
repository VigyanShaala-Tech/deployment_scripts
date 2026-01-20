from sqlalchemy import text
from deployment_scripts.connection import get_engine

engine = get_engine()

def add_constraints_and_timestamps():
    try:
        with engine.begin() as conn:

            # --------------------------------------------------
            # Add unique constraint on email (ignore if exists)
            # --------------------------------------------------
            conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'uq_student_details_email'
                    ) THEN
                        ALTER TABLE raw.student_details
                        ADD CONSTRAINT uq_student_details_email UNIQUE (email);
                    END IF;
                END $$;
            """))
            print("email unique constraint checked")

            # --------------------------------------------------
            # Tables to update
            # --------------------------------------------------
            tables = [
                "raw.student_details",
                "raw.student_registration_details",
                "raw.referral_college_professor",
                "raw.student_education"
            ]

            # --------------------------------------------------
            # 1. Add columns WITHOUT defaults (old data stays NULL)
            # --------------------------------------------------
            for table in tables:
                conn.execute(text(f"""
                    ALTER TABLE {table}
                    ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;
                """))
                print(f"timestamp columns added (no defaults) for {table}")

            # --------------------------------------------------
            # 2. Set defaults ONLY for future inserts
            # --------------------------------------------------
            for table in tables:
                conn.execute(text(f"""
                    ALTER TABLE {table}
                    ALTER COLUMN inserted_at
                        SET DEFAULT timezone('Asia/Kolkata', now()),
                    ALTER COLUMN updated_at
                        SET DEFAULT timezone('Asia/Kolkata', now());
                """))
                print(f"defaults set for future inserts on {table}")

            # --------------------------------------------------
            # 3. Function to auto-update updated_at (future updates only)
            # --------------------------------------------------
            conn.execute(text("""
                CREATE OR REPLACE FUNCTION set_updated_at()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = timezone('Asia/Kolkata', now());
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """))

            # --------------------------------------------------
            # 4. Create triggers only if they don’t already exist
            # --------------------------------------------------
            for table in tables:
                trigger_name = table.replace(".", "_") + "_updated_at_trigger"

                conn.execute(text(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM pg_trigger
                            WHERE tgname = '{trigger_name}'
                        ) THEN
                            CREATE TRIGGER {trigger_name}
                            BEFORE UPDATE ON {table}
                            FOR EACH ROW
                            EXECUTE FUNCTION set_updated_at();
                        END IF;
                    END $$;
                """))
                print(f"trigger checked for {table}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("schema update failed:", e)


if __name__ == "__main__":
    add_constraints_and_timestamps()
