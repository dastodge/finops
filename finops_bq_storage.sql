DECLARE project_id STRING DEFAULT 'bt-con-digital-dq-dp-prod';
DECLARE region STRING DEFAULT 'region-europe-west2';

DECLARE sql STRING;

SET sql = '''
  WITH table_info AS (
    SELECT
      t.table_schema,
      t.table_name,
      s.total_logical_bytes / (1024 * 1024 * 1024) AS size_gb,
      t.creation_time,
      s.storage_last_modified_time,
      t.partitioning_column,
      t.partitioning_type,
      t.clustering_columns
    FROM
      `''' || project_id || '''.''' || region || '''.INFORMATION_SCHEMA.TABLES` t
    JOIN
      `''' || project_id || '''.''' || region || '''.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT` s
    ON
      t.table_schema = s.table_schema AND t.table_name = s.table_name
  ),

  job_references AS (
    SELECT
      ref.dataset_id AS table_schema,
      ref.table_id AS table_name,
      MAX(j.creation_time) AS last_query_time
    FROM
      `''' || project_id || '''.''' || region || '''.INFORMATION_SCHEMA.JOBS_BY_PROJECT` j,
      UNNEST(j.referenced_tables) AS ref
    WHERE
      j.job_type = 'QUERY'
      AND ref.project_id = "''' || project_id || '''"
    GROUP BY
      ref.dataset_id, ref.table_id
  )

  SELECT
    i.table_schema,
    i.table_name,
    i.size_gb,
    i.creation_time,
    i.storage_last_modified_time,
    i.partitioning_column,
    i.partitioning_type,
    i.clustering_columns,
    r.last_query_time,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), i.creation_time, DAY) AS days_since_creation,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.last_query_time, DAY) AS days_since_last_query
  FROM
    table_info i
  LEFT JOIN
    job_references r
  ON
    i.table_schema = r.table_schema AND i.table_name = r.table_name
  ORDER BY
    i.size_gb DESC
''';

EXECUTE IMMEDIATE sql;