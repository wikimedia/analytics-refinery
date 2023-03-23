--
-- Aggregate from event.virtualpageview into wmf.virtualpageview_hourly.
--
-- Parameters:
--     artifacts_directory   -- Version of the jar to import for UDFs.
--     refinery_jar_version  -- The artifact directory where to find
--                              jar files to import for UDFs.
--     source_table          -- Fully qualified source table name.
--     destination_table     -- Fully qualified destination table name.
--     record_version        -- Version number of the current schema.
--     whitelist_table       -- Fully qualified table name for the whitelist
--                              to be used as filter.
--     year                  -- Year of the partition to aggregate.
--     month                 -- Month of the partition to aggregate.
--     day                   -- Day of the partition to aggregate.
--     hour                  -- Hour of the partition to aggregate.
--
-- Usage example:
--     hive -f virtualpageview_hourly.hql \
--         -d artifacts_directory=/wmf/refinery/current/artifacts \
--         -d refinery_jar_version=X.X.X \
--         -d source_table=event.virtualpageview \
--         -d destination_table=wmf.virtualpageview_hourly \
--         -d record_version=0.0.1 \
--         -d whitelist_table=wmf.pageview_whitelist \
--         -d year=2018 \
--         -d month=3 \
--         -d day=14 \
--         -d hour=0
--

SET parquet.compression = SNAPPY;
SET mapred.reduce.tasks = 8;

ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;
CREATE TEMPORARY FUNCTION get_pageview_info AS 'org.wikimedia.analytics.refinery.hive.GetPageviewInfoUDF';

WITH decorated_virtualpageviews AS (
    SELECT
        get_pageview_info(
            -- uri host
            regexp_extract(event.source_url, '^https?://([^@:/]+:[^@:/]+@)?([^:/]+)', 2),
            -- uri path
            regexp_extract(event.source_url, '^https?://[^/]+/([^?#]*)', 1),
            -- uri query
            regexp_extract(event.source_url, '^https?://[^/]+/[^?#]*\\?([^#]*)', 1)
        ) AS pageview_info,
        *
    FROM
        ${source_table}
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day} AND
        hour = ${hour}
),
filtered_virtualpageviews AS (
    SELECT * FROM decorated_virtualpageviews
    JOIN ${whitelist_table} whitelist
        ON (pageview_info['project'] = whitelist.authorized_value)
)
INSERT OVERWRITE TABLE ${destination_table} PARTITION(
    year = ${year},
    month = ${month},
    day = ${day},
    hour = ${hour}
)
SELECT
    pageview_info['project'] AS project,
    pageview_info['language_variant'] AS language_variant,
    event.page_title AS page_title,
    COALESCE(event.access_method, 'desktop') AS access_method,
    IF(useragent.is_bot, 'spider', 'user') AS agent_type,
    'internal' AS referer_class,
    geocoded_data['continent'] AS continent,
    geocoded_data['country_code'] AS country_code,
    geocoded_data['country'] AS country,
    geocoded_data['subdivision'] AS subdivision,
    geocoded_data['city'] AS city,
    map(
        'browser_family', useragent.browser_family,
        'browser_major', useragent.browser_major,
        'device_family', useragent.device_family,
        'os_family', useragent.os_family,
        'os_major', useragent.os_major,
        'os_minor', useragent.os_minor,
        'wmf_app_version', useragent.wmf_app_version
    ) AS user_agent_map,
    '${record_version}' AS record_version,
    count(*) as view_count,
    event.page_id AS page_id,
    event.page_namespace AS namespace_id,
    event.source_title AS source_page_title,
    event.source_page_id AS source_page_id,
    event.source_namespace AS source_namespace_id
FROM
    filtered_virtualpageviews
GROUP BY
    pageview_info['project'],
    pageview_info['language_variant'],
    event.page_title,
    COALESCE(event.access_method, 'desktop'),
    IF(useragent.is_bot, 'spider', 'user'),
    geocoded_data['continent'],
    geocoded_data['country_code'],
    geocoded_data['country'],
    geocoded_data['subdivision'],
    geocoded_data['city'],
    useragent,
    event.page_id,
    event.page_namespace,
    event.source_title,
    event.source_page_id,
    event.source_namespace
;
