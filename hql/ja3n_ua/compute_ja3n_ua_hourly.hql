--
-- Computes 1 hour worth of ja3n_ua_hourly data.
--
-- See table description in the companion create_ja3n_ua_hourly.hql file for more detail.
--
-- Parameters:
--     webrequest_table   Fully qualified name of the webrequest table (data source).
--     destination_table  Fully qualified name of the destination table.
--     year               Year of the partition to compute data for.
--     month              Month of the partition to compute data for.
--     day                Day of the partition to compute data for.
--     hour               Hour of the partition to compute data for.
--     num_output_files   Number of output files per partition.
--
-- Usage (test settings):
--     spark3-submit \
--         --name compute_ja3n_ua_hourly_test \
--         --master yarn \
--         --deploy-mode client \
--         --queue default \
--         --driver-cores 2 \
--         --driver-memory 4G \
--         --executor-cores 2 \
--         --executor-memory 8G \
--         --conf spark.executor.memoryOverhead=2048 \
--         --conf spark.dynamicAllocation.maxExecutors=16 \
--         --conf spark.executorEnv.SPARK_HOME=/usr/lib/spark3 \
--         --conf spark.executorEnv.SPARK_CONF_DIR=/etc/spark3/conf \
--         --conf spark.yarn.appMasterEnv.SPARK_CONF_DIR=/etc/spark3/conf \
--         --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/lib/spark3 \
--         --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
--         --class org.apache.spark.sql.hive.thriftserver.WMFSparkSQLCLIDriver \
--         hdfs:///wmf/cache/artifacts/airflow/analytics/wmf-sparksqlclidriver-1.0.0.jar \
--         -f hdfs://analytics-hadoop/user/mforns/queries/compute_ja3n_ua_hourly.hql \
--         -d webrequest_table=wmf.webrequest \
--         -d destination_table=mforns.ja3n_ua_hourly \
--         -d year=2025 \
--         -d month=12 \
--         -d day=1 \
--         -d hour=0 \
--         -d num_output_files=1
--


-- Define dt value for this partition.
SET partition_dt = TIMESTAMP(
    CONCAT(
        ${year}, "-",
        LPAD(${month}, 2, '0'), "-",
        LPAD(${day}, 2, '0'), "T",
        LPAD(${hour}, 2, '0'),
        ":00:00.000Z"
    )
);

-- Delete existing data for the partition.
DELETE
    FROM ${destination_table}
    WHERE dt = ${partition_dt}
;

-- Aggregate base data from webrequest: ja3n, user_agent and request_count.
WITH ja3n_ua_reqs AS (
    SELECT
        x_analytics_map["ja3n"] AS ja3n,
        user_agent,
        FIRST(user_agent_map) AS user_agent_map,
        COUNT(*) AS request_count
    FROM ${webrequest_table}
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day} AND
        hour = ${hour} AND
        x_analytics_map["ja3n"] IS NOT NULL AND
        user_agent IS NOT NULL AND
        user_agent != "-"
    GROUP BY
        x_analytics_map["ja3n"],
        user_agent
),

-- Add request_count and rank for ja3n and user_agent.
ja3n_ua_reqs_ext AS (
    SELECT
        *,
        SUM(request_count) OVER (PARTITION BY ja3n) AS ja3n_request_count,
        SUM(request_count) OVER (PARTITION BY user_agent) AS ua_request_count,
        ROW_NUMBER() OVER (PARTITION BY user_agent ORDER BY request_count DESC) AS ja3n_rank,
        ROW_NUMBER() OVER (PARTITION BY ja3n ORDER BY request_count DESC) AS ua_rank
    FROM ja3n_ua_reqs
),

-- Add neff for ja3n and user_agent.
ja3n_ua_reqs_ext_neff AS (
    SELECT
        *,
        1 / SUM(POWER(ja3n_share_within_ua, 2)) OVER (PARTITION BY user_agent) AS ja3n_neff,
        1 / SUM(POWER(ua_share_within_ja3n, 2)) OVER (PARTITION BY ja3n) AS ua_neff
    FROM (
        SELECT
            *,
            request_count / ua_request_count AS ja3n_share_within_ua,
            request_count / ja3n_request_count AS ua_share_within_ja3n
        FROM ja3n_ua_reqs_ext
    )
)

-- Write data to destination table.
INSERT INTO ${destination_table}
SELECT /*+ COALESCE(${num_output_files}) */
    ja3n,
    user_agent,
    user_agent_map,
    request_count,
    ja3n_rank / ja3n_neff AS ja3n_norm_rank,
    ua_rank / ua_neff AS ua_norm_rank,
    ${partition_dt} AS dt
FROM ja3n_ua_reqs_ext_neff
;
