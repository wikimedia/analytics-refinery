--
-- Extracts mobile os_family distribution metrics from pageview_hourly table,
-- and stores them in the anomaly detection table, by access method.
-- We currently only compute this metric for 'mobile web' and 'mobile app'
-- the reasons why are explained here: https://phabricator.wikimedia.org/T257692
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
--     sudo -u analytics hive -f mobile_os_distribution.hql \
--         -d refinery_hive_jar='hdfs://analytics-hadoop/some/path/to/refinery-hive-0.1.9.jar' \
--         -d source_table='wmf.pageview_hourly' \
--         -d destination_table='wmf.anomaly_detection' \
--         -d source='mobile_os_distribution' \
--         -d year=2019 \
--         -d month=9 \
--         -d day=29
--

ADD JAR ${refinery_hive_jar};
CREATE TEMPORARY FUNCTION entropy AS 'org.wikimedia.analytics.refinery.hive.EntropyUDAF';
SET dt = CONCAT('${year}-', LPAD(${month}, 2, '0'), '-', LPAD(${day}, 2, '0'));

INSERT OVERWRITE TABLE ${destination_table} PARTITION (
    source = '${source}', year = ${year}, month = ${month}, day = ${day}
)

SELECT
    ${hiveconf:dt} AS dt,
    access_method AS metric,
    entropy(counts) AS value
FROM (
    SELECT
        access_method,
        user_agent_map['os_family'],
        sum(view_count) AS counts
    FROM ${source_table}
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day} AND
        agent_type = 'user' AND
        access_method in ('mobile app', 'mobile web')
    GROUP BY
        access_method, user_agent_map['os_family']
) AS aux
GROUP BY access_method
;
