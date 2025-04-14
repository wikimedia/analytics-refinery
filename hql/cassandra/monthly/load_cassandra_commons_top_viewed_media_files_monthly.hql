-- Load commons.top_viewed_media_files_monthly dataset to cassandra.
--
-- Parameters:
--     destination_table     -- Cassandra table to write query output.
--     source_table          -- Fully qualified iceberg table to compute from.
--     year_month            -- YYYY-MM month of partition to compute from.
--     coalesce_partitions   -- number of partitions for destination data.

-- Usage:
-- spark3-submit \
-- --master yarn \
-- --conf spark.sql.catalog.aqs=com.datastax.spark.connector.datasource.CassandraCatalog \
-- --conf spark.sql.catalog.aqs.spark.cassandra.connection.host=cassandra-dev2001-a.codfw.wmnet:9042 \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.username=aqsloader \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.password=cassandra \
-- --conf spark.sql.catalog.aqs.spark.cassandra.output.batch.size.rows=1024 \
-- --jars hdfs:///wmf/cache/artifacts/airflow/analytics/spark-cassandra-connector-assembly-3.2.0-WMF-1.jar,hdfs:///wmf/cache/artifacts/airflow/analytics/refinery-job-0.2.17-shaded.jar \
-- --conf spark.dynamicAllocation.maxExecutors=128 \
-- --conf spark.yarn.maxAppAttempts=1 \
-- --conf spark.executor.memoryOverhead=3072  \
-- --executor-memory 8G \
-- --executor-cores 2 \
-- --driver-memory 4G \
-- --driver-cores 1 \
-- --class org.apache.spark.sql.hive.thriftserver.WMFSparkSQLCLIDriver \
-- --deploy-mode client \
--     hdfs:///wmf/cache/artifacts/airflow/analytics/wmf-sparksqlclidriver-1.0.0.jar \
--     -f load_cassandra_commons_top_viewed_media_files_monthly.hql \
--     -d destination_table=aqs.commons.top_viewed_media_files_monthly \
--     -d source_table=wmf_contributors.commons_pageviews_per_media_file_monthly  \
--     -d coalesce_partitions=3 \
--     -d year_month=2023-11

WITH total_pageviews_per_media_file_per_wiki AS (
    SELECT
        media_file,
        categories,
        primary_categories,
        wiki,
        SUM(pageview_count) as pageview_count,
        year_month
    FROM ${source_table}
    WHERE year_month = '${year_month}'
    GROUP BY
        media_file,
        categories,
        primary_categories,
        wiki,
        year_month
),

exploded_categories AS (
    SELECT
        media_file,
        EXPLODE(categories) as category,
        wiki,
        pageview_count,
        year_month
    FROM total_pageviews_per_media_file_per_wiki
),

exploded_categories_all_wikis AS (
    SELECT
        media_file,
        category,
        SUM(pageview_count) AS pageview_count,
        year_month
    FROM exploded_categories
    GROUP BY
        media_file,
        category,
        year_month
),

exploded_primary_categories AS (
    SELECT
        media_file,
        EXPLODE(primary_categories) as category,
        wiki,
        pageview_count,
        year_month
    FROM total_pageviews_per_media_file_per_wiki
),

exploded_primary_categories_all_wikis AS (
    SELECT
        media_file,
        category,
        SUM(pageview_count) AS pageview_count,
        year_month
    FROM exploded_primary_categories
    GROUP BY
        media_file,
        category,
        year_month
),

top_ranked_shallow_pageviews_per_category_per_wiki AS (
    SELECT *
    FROM (
        SELECT
            category,
            'shallow' as category_scope,
            wiki,
            media_file,
            pageview_count,
            year_month,
            RANK()       over (PARTITION BY category, wiki ORDER BY pageview_count DESC) AS rank,
            ROW_NUMBER() over (PARTITION BY category, wiki ORDER BY pageview_count DESC) AS row_number
        FROM exploded_categories
    )
    WHERE row_number <= 100
),

top_ranked_shallow_pageviews_per_category_for_all_wikis AS (
    SELECT *
    FROM (
        SELECT
            category,
            'shallow' as category_scope,
            'all-wikis' as wiki,
            media_file,
            pageview_count,
            year_month,
            RANK()       over (PARTITION BY category ORDER BY pageview_count DESC) AS rank,
            ROW_NUMBER() over (PARTITION BY category ORDER BY pageview_count DESC) AS row_number
        FROM exploded_categories_all_wikis
    )
    WHERE row_number <= 100
),

top_ranked_primary_pageviews_per_category_per_wiki AS (
    SELECT *
    FROM (
        SELECT
            category,
            'deep' as category_scope,
            wiki,
            media_file,
            pageview_count,
            year_month,
            RANK()       over (PARTITION BY category, wiki ORDER BY pageview_count DESC) AS rank,
            ROW_NUMBER() over (PARTITION BY category, wiki ORDER BY pageview_count DESC) AS row_number
        FROM exploded_primary_categories
    )
    WHERE row_number <= 100
),

top_ranked_primary_pageviews_per_category_for_all_wikis AS (
    SELECT *
    FROM (
        SELECT
            category,
            'deep' as category_scope,
            'all-wikis' as wiki,
            media_file,
            pageview_count,
            year_month,
            RANK()       over (PARTITION BY category ORDER BY pageview_count DESC) AS rank,
            ROW_NUMBER() over (PARTITION BY category ORDER BY pageview_count DESC) AS row_number
        FROM exploded_primary_categories_all_wikis
    )
    WHERE row_number <= 100
),

top_ranked AS (
    SELECT
        category,
        category_scope,
        wiki,
        media_file,
        pageview_count,
        rank,
        YEAR(year_month) as year,
        MONTH(year_month) as month
    FROM (
        SELECT *
        FROM top_ranked_shallow_pageviews_per_category_per_wiki
        UNION ALL
        SELECT *
        FROM top_ranked_shallow_pageviews_per_category_for_all_wikis
        UNION ALL
        SELECT *
        FROM top_ranked_primary_pageviews_per_category_per_wiki
        UNION ALL
        SELECT *
        FROM top_ranked_primary_pageviews_per_category_for_all_wikis
    )
)

INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    category,
    category_scope,
    wiki,
    media_file,
    pageview_count,
    rank,
    year,
    month
FROM top_ranked
ORDER BY
    category,
    category_scope,
    wiki,
    year,
    month,
    rank
;
