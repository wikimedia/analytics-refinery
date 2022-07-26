-- Load editors by country monthly dataset to cassandra.
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
--     -f load_cassandra_editors_bycountry_monthly.hql \
--     -d destination_table=aqs.local_group_default_T_editors_bycountry.data \
--     -d source_table=wmf.geoeditors_public_monthly  \
--     -d coalesce_partitions=6 \
--     -d year=2022 \
--     -d month=07

 WITH prepared_editors_by_country AS (
    SELECT
        project,
        activity_level,
        month,
        editors_ceil,
        country_code
    FROM ${source_table}
    WHERE
        month = CONCAT(LPAD('${year}', 4, '0'),'-', LPAD('${month}', 2, '0'))
    DISTRIBUTE BY
        project,
        activity_level,
        month
    SORT BY
        project,
        activity_level,
        month,
        editors_ceil DESC,
        country_code
)
INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    'analytics.wikimedia.org' as _domain,
    project,
    CASE
        WHEN activity_level = '5 to 99' THEN '5..99-edits'
        WHEN activity_level = '100 or more' THEN '100..-edits'
        ELSE NULL
    END as `activity-level`,
    LPAD('${year}', 4, '0') as year,
    LPAD('${month}', 2, '0') as month,
    '13814000-1dd2-11b2-8080-808080808080' as _tid,
    CONCAT('[',
        CONCAT_WS(
            ',',
            COLLECT_LIST(
                CONCAT(
                    '{"country":"', country_code,
                    '","editors-ceil":', CAST(editors_ceil AS STRING), '}'
                )
            )
        ),
    ']') as countriesJSON
FROM prepared_editors_by_country
GROUP BY
    project,
    activity_level,
    month