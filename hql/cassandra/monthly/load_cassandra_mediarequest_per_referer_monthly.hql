--  Loads mediarequest per referer monthly dataset to cassandra
-- Parameters:
--     destination_table     -- Cassandra table to write query output.
--     source_table          -- Fully qualified hive table to compute from.
--     year                   -- year of partition to compute from.
--     month                  -- month of partition to compute from.
--     coalesce_partitions    -- number of partitions for destination data.

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
-- --conf spark.dynamicAllocation.maxExecutors=128 \
-- --conf spark.yarn.maxAppAttempts=1 \
-- --conf spark.executor.memoryOverhead=3072  \
-- --executor-memory 8G \
-- --executor-cores 2 \
-- --driver-memory 4G \
-- --name mediarequest_per_referer \
--     -f load_cassandra_mediarequest_per_referer_monthly.hql \
--     -d destination_table=aqs.local_group_default_T_mediarequest_per_referer.data \
--     -d source_table=wmf.mediarequest  \
--     -d coalesce_partitions=6 \
--     -d year=2022 \
--     -d month=07

INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    'analytics.wikimedia.org' as _domain,
    COALESCE(IF(referer = 'external (search engine)', 'search-engine', referer), 'all-referers') as referer,
    COALESCE(media_classification, 'all-media-types') as media_type,
    COALESCE(agent_type, 'all-agents') as agent,
    'monthly' as granularity,
    CONCAT(LPAD(year, 4, '0'), LPAD(month, 2, '0'), '0100') as `timestamp`,
    '13814000-1dd2-11b2-8080-808080808080' as _tid,
    SUM(request_count) as requests
FROM
    ${source_table}
WHERE
    year = ${year}
    AND month = ${month}
GROUP BY
    referer,
    media_classification,
    agent_type,
    year,
    month
GROUPING SETS (
    (
        referer,
        media_classification,
        agent_type,
        year,
        month
    ),(
        referer,
        media_classification,
        year,
        month
    ),(
        referer,
        agent_type,
        year,
        month
    ),(
        media_classification,
        agent_type,
        year,
        month
    ),(
        media_classification,
        year,
        month
    ),(
        agent_type,
        year,
        month
    ),(
        referer,
        year,
        month
    ),(
        year,
        month
    )
)
