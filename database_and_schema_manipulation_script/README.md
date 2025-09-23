# database_and_schema_manipulation_scripts

This folder contains Python scripts for managing database structures and performing schema-related data operations.

This database and schema management python scripts can be divided into two main categories:

---
# DDL (Data Definition Language) Scripts

**Responsible for creating or modifying database structures:**

1. **[create\_db\_and\_db\_schema\_script.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/database_and_schema_manipulation_script/create_db_and_db_schema_script.py)**

    * Connects to the default postgres database to create a new database and checks for duplicates.
    * Then connects to the new database to create a schema if it doesn’t exist, providing success or error messages.
    
    **Usage**:
    * Run the script from the terminal with the database and schema names as arguments: **python create_db_and_schema.py <database_name> <schema_name>**

2. **[add\_column.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/database_and_schema_manipulation_script/add_column.py)**

    * Adds a new column to an existing database table with the specified name and data type.
    * Prompts the user for the full table name, column name, and column type, then executes the ALTER TABLE command.

    **Usage**
    * Run command: **python add_column.py**

    Then follow the prompts to enter:  
    **Full table name** (e.g., raw.student_details)  
    **New column name**  
    **Data type** (e.g., VARCHAR(255), INTEGER)

3. **[alter\_table\_and\_create\_enum.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/database_and_schema_manipulation_script/alter_table_and_create_enum.py)**

    * Creates ENUM types and adds primary key constraints to tables in the specified schema.
    * Ensures schema exists before performing operations.

    **Usage**
    * Run command: **python alter_table_and_create_enum.py**

4. **[create\_enum.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/database_and_schema_manipulation_script/create_enum.py)**

    * Creates enum types.

    **Usage**
    * Run command: **python create_enum.py**

5. **[create\_final\_tables\_with\_schema.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/database_and_schema_manipulation_script/create_final_tables_with_schema.py)**

    * Creates final tables in the target schema by combining and transforming data from multiple intermediate and raw tables.
    * Generates SQL queries for each table, optionally drops existing tables, and executes the creation in the database.
    * Designed to structure data for analytics and visualization, producing tables like student demography, attendance, assignments, and quizzes.

    **Usage**
    * Run command: **python create_final_tables_with_schema.py**

6. **[create\_raw\_intermediate\_indexes.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/database_and_schema_manipulation_script/create_raw_intermediate_indexes.py)**

    * Cleans intermediate and raw tables by removing duplicate records and ensuring unique indexes exist.
    * Checks for duplicates in key tables (student_assignment, student_quiz, student_session, and other raw tables) and deletes them before indexing.
    * Provides detailed logs for duplicates found ensuring reliable and consistent tables for downstream processing.

    **Usage**
    * Run command: **python create_raw_intermediate_indexes.py**

---
# DML (Data Manipulation Language) Scripts

**Responsible for inserting or updating data:**

1. **[Add\_data\_to\_new\_column.py](https://github.com/VigyanShaala-Tech/deployment_scripts/blob/main/database_and_schema_manipulation_script/Add_data_to_new_column.py)**

    * Adds a new column to an existing table and updates it with specified values for each row based on the ID column.
    * Prompts the user for table name, column name, column type, and ID column.
    * Adds the column if it doesn’t exist and updates rows using a predefined mapping of ID → value.

    **Usage**
    * Run command: **python Add_data_to_new_column.py**

    Then follow the prompts to enter:  
    **Full table name** (e.g., raw.student_details)  
    **New column name**  
    **Column type** (e.g., VARCHAR(100))  
    **ID column name** (e.g., id)