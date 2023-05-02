-- Load the pageview top_articles monthly dataset to cassandra
--
-- Parameters:
--     destination_table                   -- Cassandra table to write query output.
--     source_table                        -- Fully qualified hive table to compute from.
--     disallowed_cassandra_articles_table -- Fully qualified hive table containing article titles we don't want to
--                                            appear in the top list (ex: offensive language, DOS attack
--                                            manipulations,...).
--     year                                -- year of partition to compute from.
--     month                               -- month of partition to compute from.
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
-- --conf spark.dynamicAllocation.maxExecutors=128 \
-- --conf spark.yarn.maxAppAttempts=1 \
-- --conf spark.executor.memoryOverhead=3072  \
-- --executor-memory 8G \
-- --executor-cores 2 \
-- --driver-memory 4G \
-- --name pageview_top_articles_monthly \
-- -f load_cassandra_pageview_top_articles_monthly.hql \
-- -d destination_table=aqs.local_group_default_T_top_pageviews.data \
-- -d source_table=wmf.pageview_hourly \
-- -d disallowed_cassandra_articles_table=wmf.disallowed_cassandra_articles \
-- -d coalesce_partitions=6 \
-- -d year=2022 \
-- -d month=7


WITH unranked as (
    SELECT /*+ BROADCAST(disallowed_list) */
        source.project,
        reflect('org.json.simple.JSONObject', 'escape', regexp_replace(page_title, '\t', '')) AS page_title,
        COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') AS access,
        LPAD(year, 4, '0') as year,
        LPAD(month, 2, '0') as month,
        SUM(view_count) as views
    FROM ${source_table} source
        LEFT OUTER JOIN ${disallowed_cassandra_articles_table} disallowed_list
        ON source.project = disallowed_list.project
            AND lower(source.page_title) = lower(disallowed_list.article)
    WHERE year = ${year}
      AND month = ${month}
      AND agent_type = 'user'
      AND page_title != '-'
      AND disallowed_list.article IS NULL
    GROUP BY source.project, regexp_replace(page_title, '\t', ''), access_method, year, month
        GROUPING SETS (
            ( source.project, regexp_replace(page_title, '\t', ''), access_method, year, month),
            ( source.project, regexp_replace(page_title, '\t', ''), year, month)
        )

), ranked AS (
    SELECT
        project,
        page_title,
        access,
        year,
        month,
        views,
        rank() OVER (PARTITION BY project, access, year, month ORDER BY views DESC) as rank,
        row_number() OVER (PARTITION BY project, access, year, month ORDER BY views DESC) as rn
    FROM unranked
),
max_rank AS (
    SELECT
        project as max_rank_project,
        access as max_rank_access,
        year as max_rank_year,
        month as max_rank_month,
        rank as max_rank
    FROM ranked
    WHERE
        rn = 1001
    GROUP BY
        project,
        access,
        year,
        month,
        rank
)
INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    'analytics.wikimedia.org' as _domain,
    project as project,
    access as access,
    year as year,
    month as month,
    'all-days' as day,
    '13814000-1dd2-11b2-8080-808080808080' as _tid,
    CONCAT('[',
        CONCAT_WS(',', collect_list(
            CONCAT('{"article":"', page_title,
                '","views":', CAST(views AS STRING),
                ',"rank":', CAST(rank AS STRING), '}'))
        ),']') articlesJSON
FROM ranked
LEFT JOIN max_rank ON (
    project = max_rank_project
    AND access = max_rank_access
    AND year = max_rank_year
    AND month = max_rank_month
)
WHERE rank < COALESCE(max_rank, 1001)
GROUP BY
    project,
    access,
    year,
    month
