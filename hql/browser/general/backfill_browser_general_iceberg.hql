-- Backfills the browser general iceberg table from the original hive table
--
-- NOTE: The destination table is expected to be empty. No deletion is made
--       prior to loading the data, and if the destination table already contains
--       for the period it is backfilled, the data will be deleted with the delete statement.
--
-- Parameters:
--     browser_general_hive_table     -- Table containing source data
--     browser_general_iceberg_table  -- Table where to write newly computed data
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
--         -f backfill_browser_general_iceberg.hql \
--         -d browser_general_hive_table=wmf.browser_general \
--         -d browser_general_iceberg_table=wmf_traffic.browser_general

DELETE FROM ${browser_general_iceberg_table};

INSERT INTO ${browser_general_iceberg_table}

SELECT
    access_method,
    os_family,
    os_major,
    browser_family,
    browser_major,
    view_count,
    TO_DATE(CONCAT_WS('-', LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0')), 'yyyy-MM-dd') AS day
FROM ${browser_general_hive_table}
SORT BY day, view_count DESC
;