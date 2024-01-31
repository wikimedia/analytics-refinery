-- Backfills the session length iceberg table from the original hive table
--
-- NOTE: The destination table is expected to be empty. If the destination
--       table already contains data, it will be deleted with the delete statement.
--
-- Parameters:
--     session_length_hive_table     -- Table containing source data
--     session_length_iceberg_table  -- Table where to write newly computed data
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
--         -f backfill_session_length_iceberg.hql \
--         -d session_length_hive_table=wmf.session_length_daily \
--         -d session_length_iceberg_table=wmf_traffic.session_length

DELETE FROM ${session_length_iceberg_table};

INSERT INTO ${session_length_iceberg_table}

SELECT
    wiki,
    session_length,
    session_count,
    TO_DATE(CONCAT_WS('-', LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0')), 'yyyy-MM-dd') AS day
FROM ${session_length_hive_table}
ORDER BY day, wiki, session_length
;
