# Repository_Overview

This repository contains five main folders, each organized according to its functionality:

**[bug\_fixing\_on\_production.py](https://github.com/VigyanShaala-Tech/deployment_scripts/tree/main/bug_fixing_on_production)**  
   *Contains scripts created to address and resolve production-related issues.

**[database\_and\_schema\_manipulation\_script.py](https://github.com/VigyanShaala-Tech/deployment_scripts/tree/main/database_and_schema_manipulation_script)**  
   *Includes scripts for modifying, updating, and maintaining database schemas and related structures.

**[insert\_user\_registration.py](https://github.com/VigyanShaala-Tech/deployment_scripts/tree/main/insert_user_registration)**  
   *Updates user registration–related data received via google form.

**[monitoring\_data\_pipeline.py](https://github.com/VigyanShaala-Tech/deployment_scripts/tree/main/monitoring_data_pipeline)**  
   *Implements the data pipeline for scraping Graphy LMS data, transforming it into structured formats, and updating monitoring tables in the raw schema, intermediate schema and final schema.

**[old\_data\_insertion\_scripts.py](https://github.com/VigyanShaala-Tech/deployment_scripts/tree/main/old_data_insertion_scripts)**  
   *Stores legacy scripts used for initial or past data insertion tasks.

---

# Folder-specific Documentation

Each folder contains its own README file that provides more detailed information about the scripts located inside it, including their purpose and execution steps.


# Configuration

All folders consistently reference a shared configuration file: config.env, located at the root of the repository. This file defines the database connection parameters:

DB_HOST='my_db_host'  
DB_NAME='my_db_name'  
DB_USER='my_db_user'  
DB_PASSWORD='my_db_password'  
DB_PORT='my_db_port'  

**Important:** Replace these placeholder values with the actual production credentials before running any scripts.

# Repository Structure:

├── **bug_fixing_on_production/**  
   └── README.md  
   └── Fix_district_state_empty_value.py  
   └── INC7_upcoming_incubators_monitoring_data_insertion.py  
   └── assign_missing_ids.py  
   └── clean_emails.py  
   └── delete_emails_from_sheet.py  
   └── delete_student_id.py  
   └── remove_dupe_student_details.py  
   └── sql_update_script.py  
    
├── **database_and_schema_manipulation_script/**  
   └── README.md  
   └── Add_data_to_new_column.py  
   └── add_column.py  
   └── alter_table_and_create_enum.py  
   └── create_db_and_db_schema_script.py  
   └── create_enum.py  
   └── create_final_tables_with_schema.py  
   └── create_raw_intermediate_indexes.py  
    
├── **insert_user_registration/**  
   └── README.md  
   └── insert_new_data.py  
    
├── **monitoring_data_pipeline/**  
   └── README.md  
   └── post_cohort_repeatative_script  
      └── monitoring_data_raw_schema_tables_update_script.py  
      └── raw_schema_to_intermediate_upsert_script.py  
      └── upsertion_intermediate_to_final.py  
   └── pre_cohort_non_repeatative_script  
      └── Add_new_cohorts_names_for_upcoming_cohort.py  
      └── Update_incubator_name_based_on_email.py  
    
├── **old_data_insertion_scripts/**  
   └── README.md  
   └── data_insertion_script.py  
   └── table_creation.py  
   └── load_csvs_to_db.py  
   └── update_clean_data.py  
   └── update_course_name_INC_7_script.py  
   └── update_script_location_id.py  
    
├── config.env  
└── README.md   ← (this file)
