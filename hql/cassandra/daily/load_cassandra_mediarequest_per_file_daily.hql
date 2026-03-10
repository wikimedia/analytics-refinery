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

WITH base AS (
    SELECT
        IF(referer = 'external (search engine)', 'search-engine', referer) AS referer_key,
        regexp_replace(base_name, '\t', '') AS file_path,
        COALESCE(agent_type, 'all-agents') AS agent,
        media_classification IN ('video','audio') AS is_av,
        IF(media_classification IN ('video','audio') AND transcoding LIKE 'image_%', TRUE, FALSE) AS is_poster,
        IF(media_classification IN ('video','audio') AND (transcoding NOT LIKE 'image_%' OR transcoding IS NULL), TRUE, FALSE) AS is_play,
        request_count
    FROM ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
),
agg AS (
    SELECT
        -- Only the rollup grouping set should get the 'all-referers' label.
        -- If the source referer is NULL, the per-referer rows should keep it as NULL.
        CASE
            WHEN GROUPING(referer_key) = 1 THEN 'all-referers'
            ELSE referer_key
        END AS referer,
        file_path,
        SUM(IF(agent = 'spider', request_count, 0)) AS spider,
        SUM(IF(agent = 'user', request_count, 0)) AS `user`,
        IF(MAX(is_av), SUM(IF(is_poster AND agent = 'spider', request_count, 0)), NULL) AS poster_spider,
        IF(MAX(is_av), SUM(IF(is_poster AND agent = 'user', request_count, 0)), NULL) AS poster_user,
        IF(MAX(is_av), SUM(IF(is_play AND agent = 'spider', request_count, 0)), NULL) AS plays_spider,
        IF(MAX(is_av), SUM(IF(is_play AND agent = 'user', request_count, 0)), NULL) AS plays_user
    FROM base
    GROUP BY
        referer_key,
        file_path
    GROUPING SETS (
        (referer_key, file_path),
        (file_path)
    )
)
INSERT INTO ${destination_table}
SELECT
 /*+ COALESCE(${coalesce_partitions}) */
    'analytics.wikimedia.org' AS _domain,
    referer,
    file_path,
    'daily' AS granularity,
    CONCAT(LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), LPAD(${day}, 2, '0'), '00') AS `timestamp`,
    '13814000-1dd2-11b2-8080-808080808080' AS _tid,
    spider,
    `user`,
    poster_spider,
    poster_user,
    plays_spider,
    plays_user
FROM agg
