-- Loads mediarequest top_files daily dataset into cassandra
-- Parameters:
--     destination_table     -- Cassandra table to write query output.
--     source_table          -- Fully qualified hive table to compute from.
--     year                   -- year of partition to compute from.
--     month                  -- month of partition to compute from.
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
    -- -f load_cassandra_mediarequest_top_files_daily.hql \
    -- -d destination_table=aqs.local_group_default_T_mediarequest_top_files.data \
    -- -d source_table=wmf.mediarequest \
    -- -d coalesce_partitions=6 \
    -- -d year=2022 \
    -- -d month=07  \
    -- -d day=01

WITH ranked AS (
    SELECT
        referer,
        file_path,
        media_classification,
        year,
        month,
        day,
        requests,
        rank() OVER (PARTITION BY referer, media_classification, year, month, day ORDER BY requests DESC) as rank,
        row_number() OVER (PARTITION BY referer, media_classification, year, month, day ORDER BY requests DESC) as rn
    FROM (
        SELECT
            COALESCE(IF(referer = 'external (search engine)', 'search-engine', referer), 'all-referers') referer,
            reflect("org.json.simple.JSONObject", "escape", regexp_replace(base_name, '\t', '')) file_path,
            COALESCE(media_classification, 'all-media-types') media_classification,
            LPAD(year, 4, '0') as year,
            LPAD(month, 2, '0') as month,
            LPAD(day, 2, '0') as day,
            SUM(request_count) as requests
        FROM ${source_table}
        WHERE
            year = ${year}
            AND month = ${month}
            AND day = ${day}
            AND agent_type = 'user'
        GROUP BY referer, regexp_replace(base_name, '\t', ''), media_classification, year, month, day
        GROUPING SETS (
            (
                year,
                month,
                day,
                referer,
                regexp_replace(base_name, '\t', ''),
                media_classification
            ), (
                year,
                month,
                day,
                regexp_replace(base_name, '\t', ''),
                media_classification
            ), (
                year,
                month,
                day,
                referer,
                regexp_replace(base_name, '\t', '')
            ), (
                year,
                month,
                day,
                regexp_replace(base_name, '\t', '')
            )
        )
    ) raw
),
max_rank AS (
    SELECT
        referer as max_rank_referer,
        media_classification as max_rank_media_classification,
        year as max_rank_year,
        month as max_rank_month,
        day as max_rank_day,
        rank as max_rank
    FROM ranked
    WHERE
        rn = 1001
    GROUP BY
        referer,
        media_classification,
        year,
        month,
        day,
        rank
)
INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    'analytics.wikimedia.org' as _domain,
    referer,
    media_classification as media_type,
    year,
    month,
    day,
    '13814000-1dd2-11b2-8080-808080808080' as _tid,
    CONCAT('[',
        CONCAT_WS(',', collect_list(
            CONCAT('{"file_path":"', file_path,
                '","requests":', CAST(requests AS STRING),
                ',"rank":', CAST(rank AS STRING), '}'))
        ),']') as filesJSON
FROM ranked
LEFT JOIN max_rank ON (
    referer = max_rank_referer
    AND media_classification = max_rank_media_classification
    AND year = max_rank_year
    AND month = max_rank_month
    AND day = max_rank_day
)
WHERE rank < COALESCE(max_rank, 1001)
GROUP BY
    referer,
    media_classification,
    year,
    month,
    day