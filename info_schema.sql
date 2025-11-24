DECLARE project_list ARRAY<STRING> DEFAULT [
  'bt-con-digital-dq-dp-prod'
  -- 'bt-con-digital-dq-dp-dev',
  -- 'bt-con-digital-dq-dp-prod',
  -- 'bt-con-digital-dp-prod'
];

DECLARE region STRING DEFAULT 'region-europe-west2';
DECLARE i INT64 DEFAULT 0;
DECLARE project_id STRING;

CREATE TEMP TABLE combined_results (
  project_id STRING,
  table_schema STRING,
  table_name STRING,
  size_gb FLOAT64,
  creation_time TIMESTAMP,
  partitioning_column STRING,
  partitioning_type STRING,
  clustering_columns STRING, d
  last_query_time TIMESTAMP,
  days_since_creation INT64,
  days_since_last_query INT64
);

WHILE i < ARRAY_LENGTH(project_list) DO
  SET project_id = project_list[OFFSET(i)];

  EXECUTE IMMEDIATE '''
    WITH table_info AS (
      SELECT
        "''' || project_id || '''" AS project_id,
        t.table_schema,
        t.table_name,
        s.total_logical_bytes / (1024 * 1024 * 1024) AS size_gb,
        t.creation_time,
        s.storage_last_modified_time,
        t.partitioning_column,
        t.partitioning_type,
        ARRAY_TO_STRING(t.clustering_columns, ", ") AS clustering_columns
      FROM
        `''' || project_id || '''.''' || region || '''.INFORMATION_SCHEMA.TABLES` t
      JOIN
        `''' || project_id || '''.''' || region || '''.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT` s
      ON
        t.table_schema = s.table_schema AND t.table_name = s.table_name
      WHERE
        t.table_schema IN ('DQ_DATA_PRODUCT', 'con_cloud_dq', 'con_cloud_dq_bk', 'dq_test_zone')
    ),

    job_references AS (
      SELECT
        ref.dataset_id AS table_schema,
        ref.table_id AS table_name,
        MAX(j.creation_time) AS last_query_time
      FROM (
        SELECT *
        FROM `''' || project_id || '''.''' || region || '''.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE job_type = 'QUERY'
      ) j
      CROSS JOIN UNNEST(j.referenced_tables) AS ref
      WHERE
        ref.project_id = "''' || project_id || '''"
        AND ref.dataset_id IN ('DQ_DATA_PRODUCT', 'con_cloud_dq', 'con_cloud_dq_bk', 'dq_test_zone')
      GROUP BY
        ref.dataset_id, ref.table_id
    )

    INSERT INTO combined_results
    SELECT
      i.project_id,
      i.table_schema,
      i.table_name,
      i.size_gb,
      i.creation_time,
      i.storage_last_modified_time,
      i.partitioning_column,
      i.partitioning_type,
      i.clustering_columns,
      r.last_query_time,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ti.creation_time, DAY) AS days_since_creation,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), jr.last_query_time, DAY) AS days_since_last_query
    FROM
      table_info ti
    LEFT JOIN
      job_references jr
    ON
      ti.table_schema = jr.table_schema AND ti.table_name = jr.table_name
  ''';

  SET i = i + 1;
END WHILE;

-- Final output
SELECT * FROM combined_results
ORDER BY size_gb DESC
LIMIT 100;