--
-- Extracts os_family entropy stats by access_method from pageview_hourly table
-- and stores them in the given data quality stats table.
-- We currently only compute this metric for 'mobile web' and 'mobile app'
-- the reasons why are explained here: https://phabricator.wikimedia.org/T257692
-- Usage:
--     sudo -u analytics hive -f os_family_entropy_by_access_method.hql \
--         -d artifacts_directory='hdfs://analytics-hadoop/wmf/refinery/current/artifacts' \
--         -d refinery_jar_version=X.X.X \
--         -d source_table='wmf.pageview_hourly' \
--         -d destination_table='analytics.data_quality_stats_incoming' \
--         -d year=2019 \
--         -d month=9 \
--         -d day=29
--

ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;
CREATE TEMPORARY FUNCTION entropy AS 'org.wikimedia.analytics.refinery.hive.EntropyUDAF';

INSERT OVERWRITE TABLE ${destination_table} PARTITION (
    source_table = '${source_table}',
    query_name = 'os_family_entropy_by_access_method',
    granularity = 'daily'
)

SELECT
    CONCAT('${year}-', LPAD(${month}, 2, '0'), '-', LPAD(${day}, 2, '0'), 'T00:00:00Z') AS dt,
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
