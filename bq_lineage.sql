
-- Find dependencies for a specific table
SELECT
  referenced_project_id,
  referenced_dataset_id,
  referenced_table_id
FROM
  `project.dataset.INFORMATION_SCHEMA.REFERENCED_TABLES`
WHERE
  table_name = 'your_table_name';

-- ****************************************************************************************************
