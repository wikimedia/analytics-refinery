-- Load knowledge gaps content gap metrics into cassandra
-- Parameters:
--      destination table: The cassandra table being written to
--      source_table: the content_gap_metrics table in hive
--      date: monthly

-- Usage:
-- spark-sql \
-- --driver-cores 1 \
-- --master yarn \
-- --conf spark.sql.catalog.aqs=com.datastax.spark.connector.datasource.CassandraCatalog \
-- --conf spark.sql.catalog.aqs.spark.cassandra.connection.host=aqs1010-a.eqiad.wmnet:9042,aqs1011-a.eqiad.wmnet:9042,aqs1012-a.eqiad.wmnet:9042 \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.username=aqsloader \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.password=cassandra \
-- --conf spark.sql.catalog.aqs.spark.cassandra.output.batch.size.rows=1024 \
-- --jars /srv/deployment/analytics/refinery/artifacts/org/wikimedia/analytics/refinery/refinery-job-0.2.4-shaded.jar \
-- --conf spark.dynamicAllocation.maxExecutors=128 \
-- --conf spark.yarn.maxAppAttempts=1 \
-- --conf spark.executor.memoryOverhead=3072  \
-- --executor-memory 8G \
-- --executor-cores 2 \
-- --driver-memory 4G \
--     -f load_cassandra_knowledge_gaps_by_category_file_monthly.hql \
--     -d destination_table=aqs.local_group_default_knowledge_gaps_content_gap_metrics.data \
--     -d source_table=knowledge_gaps.content_gap_metrics \
--     -d date="2021-09"

WITH metrics_with_totals AS (
    SELECT
        regexp_replace(cw.domain_name, '.org', '') as project,
        CONCAT(regexp_replace(kgm.time_bucket, '-', ''), '01') as dt,
        kgm.category as category,
        kgm.content_gap as content_gap,
        -- we use the fancy line below to unravel the struct object create a key value mapping to metric
        -- and value fields in the resulting query return
        explode(from_json(to_json(kgm.by_category), 'map<string, string>')) as (metric, value)
        FROM
            ${source_table} as kgm
        INNER JOIN
            canonical_data.wikis as cw
            ON (kgm.wiki_db = cw.database_code)
        WHERE
            time_bucket = '${date}'

UNION ALL

    SELECT DISTINCT
        regexp_replace(cw.domain_name, '.org', '') as project,
        CONCAT(regexp_replace(kgm.time_bucket, '-', ''), '01') as dt,
        "all-categories" as category,
        kgm.content_gap,
        explode(from_json(to_json(kgm.totals), 'map<string, string>')) as (metric, value)
        FROM
            ${source_table} as kgm
          JOIN
            canonical_data.wikis as cw
            ON (kgm.wiki_db = cw.database_code)
        WHERE
            time_bucket = '${date}'
)


INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    "analytics.wikimedia.org" as _domain,
    project,
    category,
    content_gap,
    dt,
    "13814000-1dd2-11b2-8080-808080808080" as _tid,
    metric,
    value
    FROM
        metrics_with_totals