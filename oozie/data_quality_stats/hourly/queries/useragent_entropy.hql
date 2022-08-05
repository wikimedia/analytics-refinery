--
-- Extracts UserAgent entropy stats from a given EventLogging table
-- and stores them in the given data quality stats table.
--
-- Usage:
--     sudo -u analytics hive -f useragent_entropy.hql \
--         -d artifacts_directory='hdfs://analytics-hadoop/wmf/refinery/current/artifacts' \
--         -d refinery_jar_version=X.X.X \
--         -d source_table='event.navigationtiming' \
--         -d destination_table='analytics.data_quality_stats_incoming' \
--         -d year=2019 \
--         -d month=9 \
--         -d day=29 \
--         -d hour=0
--

ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;
CREATE TEMPORARY FUNCTION entropy AS 'org.wikimedia.analytics.refinery.hive.EntropyUDAF';
SET dt = CONCAT('${year}-', LPAD(${month}, 2, '0'), '-', LPAD(${day}, 2, '0'), 'T', LPAD(${hour}, 2, '0'), ':00:00Z');

WITH base_data AS (
    SELECT useragent
    FROM ${source_table}
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day} AND
        hour = ${hour} AND
        event.isOversample = false

)

INSERT OVERWRITE TABLE ${destination_table} PARTITION (
    source_table = '${source_table}',
    query_name = 'useragent_entropy',
    granularity = 'hourly'
)

SELECT
    ${hiveconf:dt} AS dt,
    'useragent_combined_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        CONCAT(
            useragent.os_family,
            useragent.browser_family,
            useragent.device_family
        ),
        count(*) AS counts
    FROM base_data
    GROUP BY CONCAT(
        useragent.os_family,
        useragent.browser_family,
        useragent.device_family
    )
) AS aux

UNION

SELECT
    ${hiveconf:dt} AS dt,
    'useragent_os_family_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        useragent.os_family,
        count(*) AS counts
    FROM base_data
    GROUP BY useragent.os_family
) AS aux

UNION

SELECT
    ${hiveconf:dt} AS dt,
    'useragent_browser_family_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        useragent.browser_family,
        count(*) AS counts
    FROM base_data
    GROUP BY useragent.browser_family
) AS aux

UNION

SELECT
    ${hiveconf:dt} AS dt,
    'useragent_device_family_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        useragent.device_family,
        count(*) AS counts
    FROM base_data
    GROUP BY useragent.device_family
) AS aux
;
