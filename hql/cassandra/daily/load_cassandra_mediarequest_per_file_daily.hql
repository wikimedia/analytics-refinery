-- Loads mediarequest per-file daily into cassandra

-- Parameters:
--     destination_table     -- Cassandra table to write query output
--     source_table          -- Fully qualified hive table to compute from.
--     year                  -- year of partition to compute from.
--     month                 -- month of partition to compute from.
--     day                    -- day of partition to compute from.
--     coalesce_partitions    -- number of partitions for destination data.
--
-- Usage:
-- spark-sql \
-- --driver-cores 1 \
-- --master yarn \
-- --conf spark.sql.catalog.aqs=com.datastax.spark.connector.datasource.CassandraCatalog \
-- --conf spark.sql.catalog.aqs.spark.cassandra.connection.host=aqs1010-a.eqiad.wmnet:9042,aqs1011-a.eqiad.wmnet:9042,aqs1012-a.eqiad.wmnet:9042 \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.username=aqsloader \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.password=cassandra \
-- --conf spark.sql.catalog.aqs.spark.cassandra.output.batch.size.rows=1024 \
-- --jars /srv/deployment/analytics/refinery/artifacts/org/wikimedia/analytics/refinery/refinery-job-0.2.4-shaded.jar  \
-- --conf spark.dynamicAllocation.maxExecutors=64 \
-- --conf spark.yarn.maxAppAttempts=1 \
-- --conf spark.executor.memoryOverhead=2048  \
-- --executor-memory 8G \
-- --executor-cores 2 \
-- --driver-memory 4G \
--      -f load_cassandra_mediarequest_per_file_daily.hql \
--      -d destination_table=aqs.local_group_default_T_mediarequest_per_file.data \
--      -d source_table=wmf.mediarequest \
--      -d coalesce_partitions=6 \
--      -d year=2022 \
--      -d month=07 \
--      -d day=01

WITH per_referer AS (
    SELECT
        'analytics.wikimedia.org' as _domain,
        IF(referer = 'external (search engine)', 'search-engine', referer) as referer,
        regexp_replace(base_name, '\t', '') as file_path,
        'daily' as granularity,
        CONCAT(LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0'), '00') as `timestamp`,
            '13814000-1dd2-11b2-8080-808080808080' as _tid,
        SUM( IF (COALESCE(agent_type, 'all-agents') = 'spider', request_count, 0)) as spider,
        SUM( IF (COALESCE(agent_type, 'all-agents') = 'user', request_count, 0)) as `user`
    FROM
        ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
    GROUP BY
        referer,
        regexp_replace(base_name, '\t', ''),
        year,
        month,
        day
), all_referers AS (
    SELECT
        'analytics.wikimedia.org' as _domain,
        'all-referers' as referer,
        regexp_replace(base_name, '\t', '') as file_path,
        'daily' as granularity,
        CONCAT(LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0'), '00') as `timestamp`,
        '13814000-1dd2-11b2-8080-808080808080' as _tid,
        SUM( IF (COALESCE(agent_type, 'all-agents') = 'spider', request_count, 0)) as spider,
        SUM( IF (COALESCE(agent_type, 'all-agents') = 'user', request_count, 0)) as `user`
    FROM
         ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
    GROUP BY
        regexp_replace(base_name, '\t', ''),
        year,
        month,
        day
)
INSERT INTO ${destination_table}
SELECT
 /*+ COALESCE(${coalesce_partitions}) */
* FROM
(
    SELECT * FROM per_referer
    UNION ALL
    SELECT * FROM all_referers
)
