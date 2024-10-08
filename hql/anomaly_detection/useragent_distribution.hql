--
-- Extracts useragent distribution metrics from an event database table,
-- (i.e. event.navigationtiming) and stores them in the anomaly detection table.
--
-- Params:
--     refinery_hive_jar       HDFS path of the refinery-hive jar (for UDFs).
--     source_table            Event table from where to extract metrics.
--     destination_table       Anomaly detection table to write metrics to.
--     source                  Name of the job producing these metrics.
--     year                    Year of the date to process.
--     month                   Month of the date to process.
--     day                     Day of the date to process.
--
-- Usage:
--     sudo -u analytics hive -f useragent_distribution.hql \
--         -d refinery_hive_jar='hdfs://analytics-hadoop/some/path/to/refinery-hive-0.1.9.jar' \
--         -d source_table='event.navigationtiming' \
--         -d destination_table='wmf.anomaly_detection' \
--         -d source='useragent_distribution' \
--         -d year=2019 \
--         -d month=9 \
--         -d day=29
--


ADD JAR ${refinery_hive_jar};
CREATE TEMPORARY FUNCTION entropy AS 'org.wikimedia.analytics.refinery.hive.EntropyUDAF';
SET dt = CONCAT('${year}-', LPAD(${month}, 2, '0'), '-', LPAD(${day}, 2, '0'));

WITH base_data AS (
    SELECT user_agent_map
    FROM ${source_table}
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day} AND
        event.isOversample = false
)

INSERT OVERWRITE TABLE ${destination_table} PARTITION (
    source = '${source}', year = ${year}, month = ${month}, day = ${day}
)

SELECT /*+ COALESCE(1) */
    ${hiveconf:dt} AS dt,
    'os_family_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        user_agent_map['os_family'],
        count(*) AS counts
    FROM base_data
    GROUP BY user_agent_map['os_family']
) AS aux

UNION ALL

SELECT /*+ COALESCE(1) */
    ${hiveconf:dt} AS dt,
    'browser_family_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        user_agent_map['browser_family'],
        count(*) AS counts
    FROM base_data
    GROUP BY user_agent_map['browser_family']
) AS aux

UNION ALL

SELECT /*+ COALESCE(1) */
    ${hiveconf:dt} AS dt,
    'device_family_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        user_agent_map['device_family'],
        count(*) AS counts
    FROM base_data
    GROUP BY user_agent_map['device_family']
) AS aux;
