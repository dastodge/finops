

-- bq_table_age.sql

DECLARE project_id STRING DEFAULT 'bt-con-digital-dp-data';  -- Replace with your project ID
DECLARE datasets ARRAY<STRING>;
DECLARE tables ARRAY<STRUCT<dataset STRING, table STRING, partition_col STRING>>;
DECLARE dataset_list STRING;

-- Step 1: Get all datasets dynamically
EXECUTE IMMEDIATE FORMAT("""
  SELECT ARRAY_AGG(schema_name)
  FROM `%s.INFORMATION_SCHEMA.SCHEMATA`
""", project_id)
INTO datasets;

-- Step 2: Build dataset list for injection
SET dataset_list = (
  SELECT STRING_AGG(FORMAT("'%s'", d), ", ")
  FROM UNNEST(datasets) AS d
);

-- Step 3: Get all tables and their partitioning column dynamically
EXECUTE IMMEDIATE FORMAT("""
  SELECT ARRAY_AGG(STRUCT(schema_name AS dataset, table_name AS table, option_value AS partition_col))
  FROM (
    SELECT schema_name, table_name,
           (SELECT option_value FROM UNNEST(options)
            WHERE option_name = 'partitioning_column') AS option_value
    FROM `%s.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE schema_name IN (%s)
  )
""", project_id, dataset_list)
INTO tables;

-- Step 4: Create temp table for results
CREATE TEMP TABLE age_results (
  dataset STRING,
  table STRING,
  partition_col STRING,
  min_ts TIMESTAMP,
  age_days INT64
);

-- Step 5: Loop through tables and compute min timestamp
FOR t IN (
  SELECT * FROM UNNEST(tables) WHERE partition_col IS NOT NULL
) DO
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO age_results
    SELECT '%s' AS dataset, '%s' AS table, '%s' AS partition_col,
           MIN(CAST(%s AS TIMESTAMP)) AS min_ts,
           TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MIN(CAST(%s AS TIMESTAMP)), DAY) AS age_days
    FROM `%s.%s.%s`
  """, t.dataset, t.table, t.partition_col, t.partition_col, t.partition_col, project_id, t.dataset, t.table);
END FOR;

-- Step 6: Show results sorted by oldest data
SELECT * FROM age_results ORDER BY age_days DESC;
------------------------
DECLARE project_id STRING DEFAULT 'bt-con-digital-dp-data';  -- Replace with your actual project ID
DECLARE datasets ARRAY<STRING>;
DECLARE tables ARRAY<STRUCT<dataset STRING, table STRING, partition_col STRING>>;
DECLARE dataset_list STRING;

-- Step 1: Get all datasets dynamically
EXECUTE IMMEDIATE FORMAT("""
  SELECT ARRAY_AGG(schema_name)
  FROM `%s.INFORMATION_SCHEMA.SCHEMATA`
""", project_id)
INTO datasets;

-- Step 2: Build dataset list for injection
SET dataset_list = (
  SELECT STRING_AGG(FORMAT("'%s'", d), ", ")
  FROM UNNEST(datasets) AS d
);

-- Step 3: Get all tables and their partitioning column dynamically
EXECUTE IMMEDIATE FORMAT("""
  SELECT ARRAY_AGG(STRUCT(schema_name AS dataset, table_name AS table, option_value AS partition_col))
  FROM (
    SELECT schema_name, table_name,
           (SELECT option_value FROM UNNEST(options)
            WHERE option_name = 'partitioning_column') AS option_value
    FROM `%s.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE schema_name IN (%s)
  )
""", project_id, dataset_list)
INTO tables;

-- Step 4: Create temp table for results
CREATE TEMP TABLE age_results (
  dataset STRING,
  table STRING,
  partition_col STRING,
  min_ts TIMESTAMP,
  age_days INT64
);

-- Step 5: Loop through tables and compute min timestamp
FOR t IN (
  SELECT * FROM UNNEST(tables) WHERE partition_col IS NOT NULL
) DO
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO age_results
    SELECT '%s' AS dataset, '%s' AS table, '%s' AS partition_col,
           MIN(CAST(%s AS TIMESTAMP)) AS min_ts,
           TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MIN(CAST(%s AS TIMESTAMP)), DAY) AS age_days
    FROM `%s.%s.%s`
  """, t.dataset, t.table, t.partition_col, t.partition_col, t.partition_col, project_id, t.dataset, t.table);
END FOR;

-- Step 6: Show results sorted by oldest data
SELECT * FROM age_results ORDER BY age_days DESC;
