--
-- Extracts traffic entropy stats by country from a given pageview_hourly table
-- and stores them in the given data quality stats table.
--
-- Usage:
--     sudo -u analytics hive -f traffic_entropy_by_country.hql \
--         -d artifacts_directory='hdfs://analytics-hadoop/wmf/refinery/current/artifacts' \
--         -d refinery_jar_version=X.X.X \
--         -d source_table='wmf.pageview_hourly' \
--         -d destination_table='analytics.data_quality_stats_incoming' \
--         -d year=2019 \
--         -d month=9 \
--         -d day=29 \
--         -d hour=13
--

ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;
CREATE TEMPORARY FUNCTION entropy AS 'org.wikimedia.analytics.refinery.hive.EntropyUDAF';
SET hive.mapred.mode = nonstrict;

INSERT OVERWRITE TABLE ${destination_table} PARTITION (
    source_table = '${source_table}',
    query_name = 'traffic_entropy_by_country',
    granularity = 'hourly'
)

SELECT
    CONCAT('${year}-', LPAD(${month}, 2, '0'), '-', LPAD(${day}, 2, '0'), 'T', LPAD(${hour}, 2, '0'), ':00:00Z') AS dt,
    country AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        p.country,
        city,
        COUNT(*) AS counts
    FROM ${source_table} AS p
    JOIN wmf.traffic_anomaly_checked_countries AS c
    ON p.country = c.country
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day} AND
        hour = ${hour} AND
        agent_type = 'user'
    GROUP BY
        p.country,
        city
) AS aux
GROUP BY country
;
