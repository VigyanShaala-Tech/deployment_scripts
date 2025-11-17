SELECT
    amd.assignment_name,
    amd."Email",
    amd.submission_status,
    amd.submitted_at,
    gis."Incubator_Batch"
FROM old.assignment_monitoring_data AS amd
LEFT JOIN old.general_information_sheet AS gis
    ON amd."Email" = gis."Email"
WHERE amd.submitted_at ~ 'T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z'
  AND gis."Incubator_Batch" IN ('Incubator 7.0', 'Incubator 8.0', 'Incubator 9.0');

DELETE FROM old.assignment_monitoring_data
WHERE submitted_at ~ 'T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z'
  AND "Email" IN (
      SELECT "Email" FROM old.general_information_sheet
      WHERE "Incubator_Batch" IN ('Incubator 7.0', 'Incubator 8.0', 'Incubator 9.0')  ---only submissions belonging to specific incubator batches (7.0, 8.0, and 9.0) are deleted.
  );


'''
SELECT *
FROM raw.student_assignment
WHERE submitted_at::text !~ '\.[0-9]+'
  AND cohort_code IN ('INC007', 'INC008', 'INC009')

DELETE FROM raw.student_assignment
WHERE submitted_at::text !~ '\.[0-9]+'
  AND cohort_code IN ('INC007', 'INC008', 'INC009')
'''

'''
SELECT *
FROM intermediate.final_assignment
WHERE submitted_at::text !~ '\.[0-9]+'
  AND cohort_code IN ('INC007', 'INC008', 'INC009')

DELETE FROM intermediate.final_assignment
WHERE submitted_at::text !~ '\.[0-9]+'
  AND cohort_code IN ('INC007', 'INC008', 'INC009')
'''