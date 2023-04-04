-- Loads pageview per-article daily  dataset to cassandra
-- Parameters:
--     destination_table                   -- Cassandra table to write query output.
--     source_table                        -- Fully qualified hive table to compute from.
--     disallowed_cassandra_articles_table -- Fully qualified hive table containing article titles we don't want to
--                                            appear in the list (ex: offensive language, DOS attack, manipulations...).
--     year                                -- year of partition to compute from.
--     month                               -- month of partition to compute from.
--     day                                 -- day of partition to compute from.
--     coalesce_partitions                 -- number of partitions for destination data.
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
-- -f load_cassandra_pageview_per_article_daily.hql  \
-- -d destination_table=aqs.local_group_default_T_pageviews_per_article_flat.data  \
-- -d source_table=wmf.pageview_hourly \
-- -d disallowed_cassandra_articles_table=wmf.disallowed_cassandra_articles \
-- -d coalesce_partitions=6 \
-- -d year=2022 \
-- -d month=07 \
-- -d day=01
--
INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    'analytics.wikimedia.org' as _domain,
    source.project,
    regexp_replace(source.page_title, '\t', '') as article,
    'daily' as granularity,
    CONCAT(LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0'), '00') as timestamp,
    '13814000-1dd2-11b2-8080-808080808080' as _tid,
    CAST(SUM(view_count) AS STRING) as aa,
    CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'automated', view_count, 0)) AS STRING) as ab,
    CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'spider', view_count, 0)) AS STRING) as as,
    CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'user', view_count, 0)) AS STRING) as au,
    CAST(SUM( IF (COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'desktop', view_count, 0)) AS STRING) as da,
    CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'desktop')
        AND (COALESCE(agent_type, 'all-agents') = 'automated'), view_count, 0)) AS STRING) as db,
    CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'desktop')
        AND (COALESCE(agent_type, 'all-agents') = 'spider'), view_count, 0)) AS STRING) as ds,
    CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'desktop')
        AND (COALESCE(agent_type, 'all-agents') = 'user'), view_count, 0)) AS STRING) as du,
    CAST(SUM( IF (COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-app', view_count, 0)) AS STRING) as maa,
    CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-app')
        AND (COALESCE(agent_type, 'all-agents') = 'automated'), view_count, 0)) AS STRING) as mab,
    CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-app')
        AND (COALESCE(agent_type, 'all-agents') = 'spider'), view_count, 0)) AS STRING) as mas,
    CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-app')
        AND (COALESCE(agent_type, 'all-agents') = 'user'), view_count, 0)) AS STRING)as mau,
    CAST(SUM( IF (COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-web', view_count, 0)) AS STRING) as mwa,
    CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-web')
        AND (COALESCE(agent_type, 'all-agents') = 'automated'), view_count, 0)) AS STRING) as mwb,
    CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-web')
        AND (COALESCE(agent_type, 'all-agents') = 'spider'), view_count, 0)) AS STRING) as mws,
    CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-web')
        AND (COALESCE(agent_type, 'all-agents') = 'user'), view_count, 0)) AS STRING) as mwu
FROM ${source_table} source
LEFT OUTER JOIN ${disallowed_cassandra_articles_table} disallowed_list
    ON source.project = disallowed_list.project
        AND lower(source.page_title) = lower(disallowed_list.article)
WHERE
    year = ${year}
    AND month = ${month}
    AND day = ${day}
    AND disallowed_list.article IS NULL
GROUP BY
    source.project,
    regexp_replace(source.page_title, '\t', ''),
    year,
    month,
    day
