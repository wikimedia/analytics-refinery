-- Backfills the interlanguage navigation from the original hive table
--
-- NOTE: The destination table is expected to be empty. No deletion is made
--       prior to loading the data, and if the destination table already contains
--       for the period it is backfilled, duplication will happen.
--
-- Parameters:
--     interlanguage_navigation_hive_table     -- Table containing source data
--     interlanguage_navigation_iceberg_table  -- Table where to write newly computed data
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
--         -f backfill_interlanguage_navigation_iceberg.hql \
--         -d interlanguage_navigation_hive_table=wmf.interlanguage_navigation \
--         -d interlanguage_navigation_iceberg_table=wmf_traffic.interlanguage_navigation

INSERT INTO ${interlanguage_navigation_iceberg_table}

SELECT
    project_family,
    current_project,
    previous_project,
    navigation_count,
    TO_DATE(date) as day
FROM ${interlanguage_navigation_hive_table}
DISTRIBUTE BY year(day)
SORT BY day, project_family, current_project, previous_project
;