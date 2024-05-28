-- Load commons.top_edited_categories_monthly dataset to cassandra.
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
--     -f load_cassandra_commons_top_edited_categories_monthly.hql \
--     -d destination_table=aqs.commons.top_edited_categories_monthly \
--     -d source_table=wmf_contributors.commons_edits  \
--     -d coalesce_partitions=3 \
--     -d year_month=2023-11

WITH date_filtered_source_table AS (
    SELECT *
    FROM ${source_table}
    WHERE dt >= TO_TIMESTAMP('${year_month}')
      AND dt <  TO_TIMESTAMP('${year_month}') + INTERVAL 1 MONTH
),

total_edits_per_edit_type AS (
    SELECT
        edit_type,
        categories,
        primary_categories,
        COUNT(1) as edit_count
    FROM date_filtered_source_table
    GROUP BY
        edit_type,
        categories,
        primary_categories
),

exploded_categories AS (
    SELECT
        edit_type,
        category,
        SUM(edit_count) as edit_count
    FROM (
         SELECT
             edit_type,
             EXPLODE(categories) AS category,
             edit_count
         FROM total_edits_per_edit_type)
    GROUP BY CUBE (edit_type, category)
    HAVING category IS NOT NULL -- remove 'all-categories' nulls as they are not aggregatable
),

exploded_primary_categories AS (
    SELECT
        edit_type,
        category,
        SUM(edit_count) as edit_count
    FROM (
         SELECT
             edit_type,
             EXPLODE(primary_categories) AS category,
             edit_count
         FROM total_edits_per_edit_type)
    GROUP BY CUBE (edit_type, category)
    HAVING category IS NOT NULL -- remove 'all-categories' nulls as they are not aggregatable
),

top_ranked_shallow_edits_per_category_per_edit_type AS (
    SELECT *
    FROM (
        SELECT
            edit_type,
            category,
            'shallow' as category_scope,
            edit_count,
            RANK()       over (PARTITION BY edit_type ORDER BY edit_count DESC) AS rank,
            ROW_NUMBER() over (PARTITION BY edit_type ORDER BY edit_count DESC) AS row_number
        FROM exploded_categories
    )
    WHERE row_number <= 1000
),

top_ranked_primary_edits_per_category_per_edit_type AS (
    SELECT *
    FROM (
        SELECT
            edit_type,
            category,
            'deep' as category_scope,
            edit_count,
            RANK()       over (PARTITION BY edit_type ORDER BY edit_count DESC) AS rank,
            ROW_NUMBER() over (PARTITION BY edit_type ORDER BY edit_count DESC) AS row_number
        FROM exploded_primary_categories
    )
    WHERE row_number <= 1000
),

top_ranked  AS (
    SELECT
        category_scope,
        COALESCE(edit_type, 'all-edit-types') as edit_type, -- GROUP BY CUBE will set value to null when it is all-edit-types
        category,
        edit_count,
        rank,
        YEAR(TO_TIMESTAMP('${year_month}')) as year,
        MONTH(TO_TIMESTAMP('${year_month}')) as month
    FROM (
        SELECT *
        FROM top_ranked_shallow_edits_per_category_per_edit_type
        UNION ALL
        SELECT *
        FROM top_ranked_primary_edits_per_category_per_edit_type
    )
)

INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    category_scope,
    edit_type,
    category,
    edit_count,
    rank,
    year,
    month
FROM top_ranked
ORDER BY
    category_scope,
    edit_type,
    year,
    month,
    rank
;
