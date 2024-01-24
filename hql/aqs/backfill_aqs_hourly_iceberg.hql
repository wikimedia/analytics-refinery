-- Backfills the aqs hourly from the original hive table
--
-- NOTE: The destination table is expected to be empty. No deletion is made
--       prior to loading the data, and if the destination table already contains
--       for the period it is backfilled, duplication will happen.
--
-- Parameters:
--     aqs_hourly_hive_table     -- Table containing source data
--     aqs_hourly_iceberg_table  -- Table where to write newly computed data
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
--         -f backfill_aqs_hourly_iceberg.hql \
--         -d aqs_hourly_hive_table=wmf.aqs_hourly \
--         -d aqs_hourly_iceberg_table=wmf_traffic.aqs_hourly

INSERT INTO ${aqs_hourly_iceberg_table}

SELECT
    cache_status,
    http_status,
    http_method,
    response_size,
    uri_host,
    uri_path,
    request_count,
    CAST(CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-',
        LPAD(day, 2, '0'), ' ',
        LPAD(hour, 2, '0'), ':00:00'
    ) AS TIMESTAMP)
    AS hour
FROM ${aqs_hourly_hive_table}
DISTRIBUTE BY day(hour)
SORT BY hour
;
