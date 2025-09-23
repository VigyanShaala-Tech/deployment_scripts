# insert_user_registration

This script ingests large volumes of Google Form registration data from CSV into the database.

1. **[insert\_new\_data.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/insert_user_registration/insert_new_data.py)**

    * Inserts bulk Google Form registration data from CSV into the GIS table in the raw schema, appending new records without overwriting existing data.
    * Uses chunked bulk inserts for efficient loading while preserving table structure and handling large datasets safely.

