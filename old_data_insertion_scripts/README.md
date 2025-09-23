# old_data_insertion_scripts

These scripts are used for inserting past data into the database to ensure completeness before live monitoring begins. They are typically **non-repetitive** and run on-demand when older data from sources like CSV downloads, or exported files needs to be backfilled.

1. **[data\_insertion\_script.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/old_data_insertion_scripts/data_insertion_script.py)**

   * Handles insertion of past records into the database.
   * Ensures consistency with schema constraints while loading historical datasets.

2. **[load\_csvs\_to\_db.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/old_data_insertion_scripts/load_csvs_to_db.py)**

    * Reads all CSV files from the provided folder path.  
    * Automatically creates tables in the **raw schema** of the database, using CSV file names as table names (spaces replaced with underscores, converted to lowercase).  
    * Ensures existing tables are not overwritten (`if_exists="fail"`).  
    * Facilitates quick ingestion of multiple CSV datasets into the raw layer for further processing. 

3. **[table\_creation.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/old_data_insertion_scripts/table_creation.py)**

    * Handles creating database tables with well-defined structures.
    * Defines proper columns, data types, and constraints (e.g., primary/foreign keys).
    * Ensures tables exist before data insertion
    * Maintains data integrity by enforcing relationships and valid values.

---
# data_correction_scripts

4. **[update\_clean\_data.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/old_data_insertion_scripts/update_clean_data.py)**

    * Updates cleaned values from a CSV file into the target columns of the GIS table within the raw schema.
    * Uses Student_id as the key to match records in the database.
    * Updates only non-null values from the CSV, preventing overwriting of valid existing data.

5. **[update\_course\_name\_INC_7\_script.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/old_data_insertion_scripts/update_course_name_INC_7_script.py)**

    * Updates the Incubator_Course_Name in the GIS table by matching Student_id and applying only non-null cleaned values.

6. **[update\_script\_location\_id.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/old_data_insertion_scripts/update_script_location_id.py)**
    
    * Updates the location_id in the StudentDetails table.
    * Uses trim() and lower() to remove leading/trailing spaces and handle case differences for accurate matching.
    * Matches students with the correct location by joining GeneralInformationSheet and LocationMapping on state/union territory and district.