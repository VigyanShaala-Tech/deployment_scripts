# bug_fixing_on_production

This folder contains a collection of Python scripts designed to **identify, debug, and resolve data-related issues directly in the production environment**. These scripts are primarily focused on **cleaning, updating, and correcting inconsistencies in the production database or data pipeline outputs** to ensure data integrity.

This folder contains scripts used for production-level data maintenance — primarily focused on two major tasks:  
1. **Data Cleaning & Standardization** – Fixing inconsistencies, removing duplicates, and correcting invalid entries.  
2. **Data Correction** – Repairing database records, filling missing values, and updating data directly in production.

---

# Data Correction

1. **[Fix_district_state_empty_value.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/bug_fixing_on_production/Fix_district_state_empty_value.py)**

    * Fills empty district/state fields in incubator batches 4–7.
    * Uses older data and compares it against a standardized reference file. 

    **Usage**
    * Run command: **python Fix_district_state_empty_value.py**

2. **[assign_missing_ids.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/bug_fixing_on_production/assign_missing_ids.py)**

    * Assigns missing student_id values in raw.general_information_sheet.
    * Uses the last valid student_id and assigns it to rows with missing IDs.
    * Script was used one-time before pushing data into the intermediate table, where student_id mapping was required.
    
    **Usage**:
    * Run command: **python assign_missing_ids.py**

3. **[INC7\_upcoming\_incubators\_monitoring\_data\_insertion.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/bug_fixing_on_production/INC7_upcoming_incubators_monitoring_data_insertion.py)**

    * Inserts missing monitoring data for Incubator 7 (INC7).
    * Fixes issues in mapping for student_assignment, student_session, and student_quiz tables.
    * Runs in three steps – once for each table by commenting out.

    **Usage**
    * Run command: **python INC7_upcoming_incubators_monitoring_data_insertion.py**

4. **[sql_update_script.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/bug_fixing_on_production/bug_fixing_on_production/sql_update_script.py)**

    * Updates existing values in a table based on user input.

    **Usage**
    * Run command: **python sql_update_script.py**

    Then follow the prompts to enter:  
        **Full table name** (e.g., raw.student_details)  
        **Column to update**
        **New Value**  
        **Where column** 
        **Where value** 

---

# Data Cleaning & Standardization

1. **[clean_emails.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/bug_fixing_on_production/clean_emails.py)**

    * Standardizes email IDs by converting them to lowercase.
    * Runs on both raw.general_information_sheet and intermediate.student_details.
    **Usage**
    * Run command: **python clean_emails.py**

2. **[delete_emails_from_sheet.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/bug_fixing_on_production/delete_emails_from_sheet.py)**

    * Removes duplicate email IDs from raw.general_information_sheet.
    * Ensures email uniqueness before assigning student_id.
    **Usage**
    * Run command: **python delete_emails_from_sheet.py**

3. **[delete_student_id.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/bug_fixing_on_production/delete_student_id.py)**

    * Deletes students with specified IDs from all related tables.
    * Creates a backup of deleted records into a CSV file.

    **Usage**
    * Run command: **python delete_student_id.py**

4. **[remove\_dupe\_student\_details.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/bug_fixing_on_production/remove_dupe_student_details.py)**

    * Removes duplicate student detail entries caused by a bug in the student registration UI form.
    * Retains only the most recent student_id.
    * Saves a backup of deleted records for reference.

    **Usage**
    * Run command: **python create_raw_intermediate_indexes.py**

