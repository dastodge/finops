-- tables not modified in 60 days 
SELECT 
    *,  
    DATE(storage_last_modified_time) AS storage_last_modified_date
FROM `bt-bvp-data-qual-dp-prod.qlik_source_pii.tbl_storage_stale_60d`
WHERE 
    (project_id LIKE 'bt-con-digital-dp%'
     OR project_id LIKE ('bt-adobe-event-ext%')
     OR project_id LIKE ('bt-con-ga360-an-ext%')
     OR project_id LIKE ('bt-con-market%')
     OR project_id IN (
        'bt-con-common-data',
        'bt-con-custexp-r-data')
    )
    AND snapshot_date = CURRENT_DATE()
        -- '2025-10-10'
ORDER BY storage_last_modified_time ASC
;

-------------------------------------------------------------------------------------------
