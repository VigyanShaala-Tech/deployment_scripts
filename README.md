# deployment_scripts

This repository contains Python scripts for the **end-to-end data processing pipeline** that transforms old, unstructured data from multiple sources — including **Google Sheets, Graphy LMS, UI inputs, CSV downloads from the app, and attendance form data** — into structured database tables.

The pipeline scripts bring all data into one place and are categorized into:

* **Repetitive scripts**: Automated scripts for live monitoring data (assignments, quizzes, sessions).
* **Non-repetitive scripts**: Utility scripts for one-time updates or structural changes.

---

## Repetitive Scripts

These scripts handle **continuous upserts** to keep live monitoring data updated across schema layers:

1. **[monitoring\_data\_raw\_schema\_tables\_update\_script.py](./monitoring_data_raw_schema_tables_update_script.py)**

   * Imports the latest monitoring data from the folder containing CSV files populated by the pipeline.
   * Updates only new data (avoiding conflicts with unique column fields).
   * Inserts into monitoring data tables within the **raw schema**.

2. **[raw\_schema\_to\_intermediate\_upsert\_script.py](./raw_schema_to_intermediate_upsert_script.py)**

   * Upserts data from **raw schema** tables into the **intermediate schema**.
   * Ensures intermediate tables are structured and ready for downstream processing.

3. **[upsertion\_intermediate\_to\_final.py](./upsertion_intermediate_to_final.py)**

   * Upserts data from **intermediate tables** to the **final schema**.
   * Final schema tables are optimized for **visualization and live dashboard updates**.


**Order of Execution:**

1)**[monitoring\_data\_raw\_schema\_tables\_update\_script.py](./monitoring_data_raw_schema_tables_update_script.py)**
2)**[raw\_schema\_to\_intermediate\_upsert\_script.py](./raw_schema_to_intermediate_upsert_script.py)**
3)**[upsertion\_intermediate\_to\_final.py](./upsertion_intermediate_to_final.py)**

---

## Non-Repetitive Scripts

These scripts are used for **manual updates or one-time structural modifications**:

1. **[Add\_new\_cohorts\_names\_for\_upcoming\_cohort.py](./Add_new_cohorts_names_for_upcoming_cohort.py)**

   * Adds new cohort names into the **cohort table** within the intermediate schema.

2. **[Update\_incubator\_name\_based\_on\_email.py](./Update_incubator_name_based_on_email.py)**

   * Updates the **incubator batch name** for students.
   * Matches data against the registered student list received from the operations team.


---

## Data Sources

* **Google Sheets**
* **Graphy LMS Platform**
* **UI Inputs**
* **CSV Downloads from the App**
* **Attendance Form Data**

---

## Schema Flow

1. **Raw Schema** → Stores initial unstructured/unprocessed data.
2. **Intermediate Schema** → Holds structured, standardized tables.
3. **Final Schema** → Optimized for dashboards and reporting (live visualization).

---

## Deployment

* Repetitive scripts should be scheduled (via cron/airflow) to ensure live data updates.
* Non-repetitive scripts should be run manually as needed.
