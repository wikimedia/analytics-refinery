--
-- Extracts traffic distribution metrics by country from pageview_hourly table,
-- and stores them in the anomaly detection table.
--
-- Params:
--     refinery_hive_jar       HDFS path of the refinery-hive jar (for UDFs).
--     source_table            Pageview hourly table where to extract metrics.
--     destination_table       Anomaly detection table to write metrics to.
--     source                  Name of the job producing these metrics.
--     year                    Year of the date to process.
--     month                   Month of the date to process.
--     day                     Day of the date to process.
--
-- Usage:
--     sudo -u analytics hive -f traffic_distribution.hql \
--         -d refinery_hive_jar='hdfs://analytics-hadoop/some/path/to/refinery-hive-0.1.9.jar' \
--         -d source_table='wmf.pageview_hourly' \
--         -d destination_table='wmf.anomaly_detection' \
--         -d source='traffic_distribution' \
--         -d year=2019 \
--         -d month=9 \
--         -d day=29
--

ADD JAR ${refinery_hive_jar};
CREATE TEMPORARY FUNCTION entropy AS 'org.wikimedia.analytics.refinery.hive.EntropyUDAF';
SET dt = CONCAT('${year}-', LPAD(${month}, 2, '0'), '-', LPAD(${day}, 2, '0'));
SET hive.mapred.mode = nonstrict;

INSERT OVERWRITE TABLE ${destination_table} PARTITION (
    source = '${source}', year = ${year}, month = ${month}, day = ${day}
)

SELECT
    ${hiveconf:dt} AS dt,
    country AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        -- Use the country name from traffic_anomaly_checked_countries
        -- instead of the country name from pageview_hourly,
        -- since the latter is calculated via geocoding
        -- and can change with MaxMind updates, breaking the metrics.
        c.country,
        city,
        COUNT(*) AS counts
    FROM ${source_table} AS p
    JOIN wmf.traffic_anomaly_checked_countries AS c
    -- Join on country_code instead of country (name)
    -- for the same reason as explained in the select above.
    ON p.country_code = c.country_code
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day} AND
        agent_type = 'user'
    GROUP BY
        c.country,
        city
) AS aux
GROUP BY country
;
