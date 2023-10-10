-- Backfills the monthly per-project-family unique devices from the original hive table
--
-- NOTE: The destination table is expected to be empty. No deletion is made
--       prior to loading the data, and if the destionation table already contains
--       for the period it is backfilled, duplication will happen.
--
-- Parameters:
--     unique_devices_hive_table     -- Table containing source data
--     unique_devices_iceberg_table  -- Table where to write newly computed data
--
-- Usage:
--     spark3-sql \
--         --master yarn \
--         --deploy-mode client \
--         --driver-cores 1 \
--         --driver-memory 4G \
--         --executor-cores 2 \
--         --executor-memory 8G \
--         --conf spark.dynamicAllocation.maxExecutors=64 \
--         --conf spark.yarn.executor.memoryOverhead=2048 \
--         --conf spark.yarn.maxAppAttempts=1 \
--         -f backfill_unique_devices_per_project_family_monthly_iceberg.hql \
--         -d unique_devices_hive_table=wmf.unique_devices_per_project_family_monthly \
--         -d unique_devices_iceberg_table=wmf_readership.unique_devices_per_project_family_monthly \

INSERT INTO ${unique_devices_iceberg_table}

SELECT /*+ COALESCE(1) */
    project_family,
    country,
    country_code,
    uniques_underestimate,
    uniques_offset,
    uniques_estimate,
    TO_DATE(CONCAT_WS('-', LPAD(year, 4, '0'), LPAD(month, 2, '0'), '01'), 'yyyy-MM-dd') AS day
FROM ${unique_devices_hive_table}
ORDER BY day, project_family, country_code
;
