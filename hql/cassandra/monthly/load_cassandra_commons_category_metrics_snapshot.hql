-- Load commons.category_metrics_snapshot monthly dataset to cassandra.
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
--     -f load_cassandra_commons_category_metrics_snapshot.hql \
--     -d destination_table=aqs.commons.category_metrics_snapshot \
--     -d source_table=wmf_contributors.commons_category_metrics_snapshot  \
--     -d coalesce_partitions=3 \
--     -d year_month=2023-11

INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    category,
    media_file_count,
    media_file_count_deep,
    used_media_file_count,
    used_media_file_count_deep,
    leveraging_wiki_count,
    leveraging_wiki_count_deep,
    leveraging_page_count,
    leveraging_page_count_deep,
    TO_TIMESTAMP(year_month) as dt
FROM ${source_table}
WHERE
    year_month = '${year_month}'
ORDER BY
    category,
    dt
;
