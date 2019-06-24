--
-- Extracts data quality metrics from the EventCapsule of a given EventLogging
-- table and stores them in the given data quality hourly table.
--
-- Usage:
--     hive -f eventcapsule_metrics.hql \
--         -d artifacts_directory='hdfs://analytics-hadoop/wmf/refinery/current/artifacts' \
--         -d refinery_jar_version='0.0.91' \
--         -d source_table='event.navigationtiming' \
--         -d destination_table='wmf.data_quality_hourly' \
--         -d year=2019 \
--         -d month=9 \
--         -d day=29 \
--         -d hour=1
--

ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;
CREATE TEMPORARY FUNCTION entropy AS 'org.wikimedia.analytics.refinery.hive.EntropyUDAF';

WITH base_data AS (
    SELECT useragent
    FROM ${source_table}
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day} AND
        hour = ${hour}
)

INSERT OVERWRITE TABLE ${destination_table} PARTITION (
    source_table = '${source_table}',
    query_name = 'eventcapsule_metrics',
    year = ${year},
    month = ${month},
    day = ${day},
    hour = ${hour}
)

SELECT
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

UNION ALL

SELECT
    'useragent_os_family_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        useragent.os_family,
        count(*) AS counts
    FROM base_data
    GROUP BY useragent.os_family
) AS aux

UNION ALL

SELECT
    'useragent_browser_family_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        useragent.browser_family,
        count(*) AS counts
    FROM base_data
    GROUP BY useragent.browser_family
) AS aux

UNION ALL

SELECT
    'useragent_device_family_entropy' AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        useragent.device_family,
        count(*) AS counts
    FROM base_data
    GROUP BY useragent.device_family
) AS aux;
