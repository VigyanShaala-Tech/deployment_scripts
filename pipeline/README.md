# pipeline scripts

This repository contains Python scripts for the **end-to-end data processing pipeline** 

The pipeline scripts bring all data into one place and are categorized into:

* **post_cohort_repeatative_scripts**: Automated scripts for live monitoring data (assignments, quizzes, sessions).
* **pre_cohort_non_repeatative_scripts**: Utility scripts for one-time updates or structural changes.

---
## post_cohort_repeatative_scripts

These scripts handle **continuous upserts** to keep live monitoring data updated across schema layers:

1. **[monitoring\_data\_raw\_schema\_tables\_update\_script.py](./deployment_scripts/pipeline/post_cohort_repeatative_script/monitoring_data_raw_schema_tables_update_script.py)**

   * Imports the latest monitoring data from the folder containing CSV files populated by the pipeline.
   * Updates only new data (avoiding conflicts with unique column fields).
   * Inserts into monitoring data tables within the **raw schema**.

2. **[raw\_schema\_to\_intermediate\_upsert\_script.py](./deployment_scripts/pipeline/post_cohort_repeatative_script/raw_schema_to_intermediate_upsert_script.py)**

   * Upserts data from **raw schema** tables into the **intermediate schema**.
   * Ensures intermediate tables are structured and ready for downstream processing.

3. **[upsertion\_intermediate\_to\_final.py](./deployment_scripts/pipeline/post_cohort_repeatative_script/upsertion_intermediate_to_final.py)**

   * Upserts data from **intermediate tables** to the **final schema**.
   * Final schema tables are optimized for **visualization and live dashboard updates**.

**Order of Execution:**

1)**[monitoring\_data\_raw\_schema\_tables\_update\_script.py](./deployment_scripts/pipeline/post_cohort_repeatative_script/monitoring_data_raw_schema_tables_update_script.py)**  
2)**[raw\_schema\_to\_intermediate\_upsert\_script.py](./deployment_scripts/pipeline/post_cohort_repeatative_script/raw_schema_to_intermediate_upsert_script.py)**  
3)**[upsertion\_intermediate\_to\_final.py](./deployment_scripts/pipeline/post_cohort_repeatative_script/upsertion_intermediate_to_final.py)**


## pre_cohort_non_repeatative_scripts

These scripts are used for **manual updates or one-time structural modifications**:

1. **[Add\_new\_cohorts\_names\_for\_upcoming\_cohort.py](./deployment_scripts/pipeline/pre_cohort_non_repeatative_script/Add_new_cohorts_names_for_upcoming_cohort.py)**

   * Adds new cohort names into the **cohort table** within the intermediate schema.

2. **[Update\_incubator\_name\_based\_on\_email.py](./deployment_scripts/pipeline/pre_cohort_non_repeatative_script/Update_incubator_name_based_on_email.py)**

   * Updates the **incubator batch name** for students.
   * Matches data against the registered student list received from the operations team.
