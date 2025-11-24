-- ****************************************************************************************************

SELECT
    usage_date,
    invoice_month,
    project_id,
    service_name,
    sku,
    usage_unit,
    label_environment,
    ROUND(SUM(usage_quantity),2) sum_usage_quantity,
    ROUND(SUM(gross_cost),2) sum_gross_cost_usd,
    ROUND(SUM(net_cost),2) sum_net_cost_usd,
    ROUND(SUM(usage_cost),2) sum_usage_cost_usd,
FROM `bt-billing-account-all.bt_central_billing.gcp_finops_billing`
WHERE usage_date >='2025-04-01'
    AND label_project_owner='610064744'
--   AND project_id in('bt-con-digital-dq-dp-prod', 'bt-bvp-data-qual-dp-prod')
    AND SKU NOT LIKE 'BigQuery Enterprise Edition%'
GROUP BY ALL
HAVING ROUND(SUM(net_cost),2) > 0 
ORDER BY 1 ASC, 3, 4, 5



-- ****************************************************************************************************

SELECT *
FROM `bt-con-digital-dq-dp-prod.region-europe-west2.INFORMATION_SCHEMA.TABLES` 
;
  
SELECT *
FROM `bt-con-digital-dq-dp-prod.region-europe-west2.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT` 
  ;

-- ****************************************************************************************************

SELECT
  -- SKU,
  project_id,
  SKU,
  -- date(usage_date) usage_date,
  ROUND(SUM(usage_quantity),2) sum_usage_quantity,
  ROUND(SUM(gross_cost),2) sum_gross_cost_usd,
  ROUND(SUM(net_cost),2) sum_net_cost_usd,
  ROUND(SUM(usage_quantity) / SUM(net_cost),2) as net_cost_per_GiB,
  ROUND((SUM(net_cost) / SUM(usage_quantity)),2) AS net_cost_per_gib_per_month,
FROM
  `bt-billing-account-all.bt_central_billing.gcp_finops_billing`
WHERE
  usage_date BETWEEN '2025-05-01' AND '2025-05-31'
  AND project_id in('bt-con-digital-dq-dp-prod', 'bt-bvp-data-qual-dp-prod')
-- AND SKU NOT LIKE 'BigQuery Enterprise Edition%'


  -- and upper(SKU) like '%PLEX%'
  -- AND SKU in ('Long-Term Physical Storage (europe-west2)','Long Term Logical Storage (europe-west2)',
  --                   'Active Physical Storage (europe-west2)','Active Logical Storage (europe-west2)')
GROUP BY
  ALL
HAVING
  ROUND(SUM(net_cost),2) > 0 
ORDER BY
  1 ASC
;


SELECT
 *
FROM
  `bt-billing-account-all.bt_central_billing.gcp_finops_billing`
WHERE
  usage_date BETWEEN '2025-08-01' AND '2025-08-01';

CREATE OR REPLACE TABLE work.temp_finops_20250714 AS
SELECT 
  label_cost_centre,
  project_id,
  project_name,
  service_name,
  sku,
  usage_unit,
  label_project_owner,
  label_project_type,
  label_environment,
  invoice_month,
  ROUND(SUM(usage_quantity),2) sum_usage_quantity,
  ROUND(SUM(gross_cost),2) sum_gross_cost_usd,
  ROUND(SUM(net_cost),2) sum_net_cost_usd,
  ROUND(SUM(usage_quantity) / SUM(net_cost),2) as net_cost_per_unit,
  ROUND((SUM(net_cost) / SUM(usage_quantity)),2) AS net_cost_per_unit_per_month
FROM `bt-billing-account-all.bt_central_billing.gcp_finops_billing`
WHERE
  usage_date BETWEEN '2025-01-01' AND '2025-08-31'
  -- AND project_id in('bt-con-digital-dq-dp-prod', 'bt-bvp-data-qual-dp-prod')
-- AND SKU NOT LIKE 'BigQuery Enterprise Edition%'

-- and upper(SKU) like '%PLEX%'
-- AND SKU in ('Long-Term Physical Storage (europe-west2)','Long Term Logical Storage (europe-west2)',
--                   'Active Physical Storage (europe-west2)','Active Logical Storage (europe-west2)')
  AND UPPER(service_name) = 'CLOUD COMPOSER'
GROUP BY ALL
HAVING ROUND(SUM(net_cost),2) > 0 
ORDER BY 1,2,3,4
;

-- ****************************************************************************************************

CREATE OR REPLACE TABLE C45_2025_DQ_analysis.jr_temp_finops_ft_20251006 AS
SELECT 
  *
FROM `bt-billing-account-all.bt_central_billing.gcp_finops_billing`
WHERE
  usage_date BETWEEN '2025-10-01' AND '2025-10-02'
  AND project_id IN(
    -- 'bt-con-market-c-proc', 'bt-con-market-c-dev-proc', 
    'bt-con-digital-dp-proc')
-- AND SKU NOT LIKE 'BigQuery Enterprise Edition%'

-- and upper(SKU) like '%PLEX%'
-- AND SKU in ('Long-Term Physical Storage (europe-west2)','Long Term Logical Storage (europe-west2)',
--                   'Active Physical Storage (europe-west2)','Active Logical Storage (europe-west2)')
  -- AND UPPER(service_name) = 'CLOUD COMPOSER'
-- GROUP BY ALL
-- HAVING ROUND(SUM(net_cost),2) > 0 
ORDER BY 1,2,3,4
;

-- ****************************************************************************************************

CREATE OR REPLACE TABLE C45_2025_DQ_analysis.jr_temp_finops_20251002 AS
SELECT
  usage_date,
  project_id,
  project_no,
  project_name,
  resource_name,
  service_name,
  sku,
  usage_unit,
  label_cost_centre,
  label_project_type,
  label_environment,
  ROUND(SUM(usage_quantity),2) sum_usage_quantity,
  ROUND(SUM(gross_cost),2) sum_gross_cost_usd,
  ROUND(SUM(net_cost),2) sum_net_cost_usd,
  ROUND(SUM(usage_quantity) / SUM(net_cost),2) as net_cost_per_unit,
  ROUND((SUM(net_cost) / SUM(usage_quantity)),2) AS net_cost_per_unit_per_month
FROM `bt-billing-account-all.bt_central_billing.gcp_finops_billing`
WHERE
  usage_date BETWEEN '2025-08-01' AND '2025-10-01'
  AND project_id IN('bt-con-market-c-proc', 'bt-con-market-c-dev-proc', 'bt-con-digital-dp-proc')
-- AND SKU NOT LIKE 'BigQuery Enterprise Edition%'

-- and upper(SKU) like '%PLEX%'
-- AND SKU in ('Long-Term Physical Storage (europe-west2)','Long Term Logical Storage (europe-west2)',
--                   'Active Physical Storage (europe-west2)','Active Logical Storage (europe-west2)')
  -- AND UPPER(service_name) = 'CLOUD COMPOSER'
GROUP BY ALL
HAVING ROUND(SUM(net_cost),2) > 0 
ORDER BY 2,1,3,4
;

-- ****************************************************************************************************

DECLARE active_logical_gib_price FLOAT64 DEFAULT 0.02;
DECLARE long_term_logical_gib_price FLOAT64 DEFAULT 0.01;
DECLARE active_physical_gib_price FLOAT64 DEFAULT 0.04;
DECLARE long_term_physical_gib_price FLOAT64 DEFAULT 0.02;
 
WITH
storage_sizes AS (
   SELECT
     table_schema AS dataset_name,
     -- Logical
     SUM(active_logical_bytes) / power(1024, 3) AS active_logical_gib,
     SUM(long_term_logical_bytes) / power(1024, 3) AS long_term_logical_gib,
     -- Physical
     SUM(active_physical_bytes) / power(1024, 3) AS active_physical_gib,
     SUM(active_physical_bytes - time_travel_physical_bytes - fail_safe_physical_bytes) / power(1024, 3) AS active_no_tt_no_fs_physical_gib,
     SUM(long_term_physical_bytes) / power(1024, 3) AS long_term_physical_gib,
     -- Restorable previously deleted physical
     SUM(time_travel_physical_bytes) / power(1024, 3) AS time_travel_physical_gib,
     SUM(fail_safe_physical_bytes) / power(1024, 3) AS fail_safe_physical_gib,
   FROM
     `region-europe-west2`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
   WHERE total_logical_bytes > 0
     AND total_physical_bytes > 0
     -- Base the forecast on base tables only for highest precision results
     AND table_type  = 'BASE TABLE'
     GROUP BY 1
)
SELECT
  dataset_name,
  -- Logical
  ROUND(
    ROUND(active_logical_gib * active_logical_gib_price, 2) + 
    ROUND(long_term_logical_gib * long_term_logical_gib_price, 2)
  , 2) as total_logical_cost,
  -- Physical
  ROUND(
    ROUND(active_physical_gib * active_physical_gib_price, 2) +
    ROUND(long_term_physical_gib * long_term_physical_gib_price, 2)
  , 2) as total_physical_cost
FROM
  storage_sizes

-- ****************************************************************************************************

SELECT
    ref.dataset_id AS table_schema,
    ref.table_id AS table_name,
    MAX(j.creation_time) AS last_query_time
FROM (
    SELECT *
    FROM `bt-con-digital-dq-dp-prod.region-europe-west2.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE job_type = 'QUERY'
    ) j
    CROSS JOIN UNNEST(j.referenced_tables) AS ref
WHERE ref.project_id = "''' || project_id || '''"
    AND j.creation_time > '2025-09-21'
GROUP BY ref.dataset_id, ref.table_id


-- ****************************************************************************************************

-- ****************************************************************************************************