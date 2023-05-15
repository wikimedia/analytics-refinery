-- Extracts one day of formatted minutely banner activity to be loaded in Druid
--
-- Usage:
--     spark-sql -f generate_daily_druid_banner_activity.hql \
--         -d source_table=wmf.webrequest \
--         -d destination_table=tmp_banner_activity_2023_01_01 \
--         -d destination_directory=/wmf/tmp/druid/daily_banner_activity \
--         -d coalesce_partitions=1 \
--         -d year=2023 \
--         -d month=1 \
--         -d day=1


DROP TABLE IF EXISTS ${destination_table};
CREATE TABLE IF NOT EXISTS ${destination_table} (
    `dt`                             string,
    `campaign`                       string,
    `banner`                         string,
    `project`                        string,
    `uselang`                        string,
    `bucket`                         string,
    `anonymous`                      boolean,
    `status_code`                    string,
    `country`                        string,
    `country_matches_geocode`        boolean,
    `region`                         string,
    `device`                         string,
    `sample_rate`                    float,
    `request_count`                  bigint,
    `normalized_request_count`       bigint
)
USING PARQUET
OPTIONS ('compression'='gzip')
LOCATION '${destination_directory}';


WITH filtered_data AS (
    SELECT
        CONCAT(SUBSTRING(dt, 0, 17), '00Z') AS dt,
        concat('http://bla.org/woo/', uri_query) AS url,
        geocoded_data
    FROM
        ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
        AND webrequest_source = 'text'
        -- drop requests with no timestamps
        AND dt != '-'
        AND uri_path = '/beacon/impression'
        AND agent_type = 'user'
        -- TODO: add once added to webrequest
        -- AND x_analytics_map['proxy'] IS NULL
)


INSERT OVERWRITE TABLE ${destination_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
    dt,
    parse_url(url, 'QUERY', 'campaign') AS campaign,
    parse_url(url, 'QUERY', 'banner') AS banner,
    parse_url(url, 'QUERY', 'project') AS project,
    parse_url(url, 'QUERY', 'uselang') AS uselang,
    parse_url(url, 'QUERY', 'bucket') AS bucket,
    parse_url(url, 'QUERY', 'anonymous') = 'true' AS anonymous,
    parse_url(url, 'QUERY', 'statusCode') AS status_code,
    parse_url(url, 'QUERY', 'country') AS country,
    geocoded_data['country_code'] = parse_url(url, 'QUERY', 'country') AS country_matches_geocode,
    geocoded_data['subdivision'] AS region,
    parse_url(url, 'QUERY', 'device') AS device,
    cast(parse_url(url, 'QUERY', 'recordImpressionSampleRate') AS float) AS sample_rate,
    COUNT(*) AS request_count,
    cast(COUNT(*) / cast(parse_url(url, 'QUERY', 'recordImpressionSampleRate') AS float) AS bigint) AS normalized_request_count
FROM
    filtered_data
WHERE
    parse_url(url, 'QUERY', 'debug') = 'false'
    -- sample_rate can be infinity, leading to Druid indexation failing.
    -- We remove those rows from the data
    AND cast(parse_url(url, 'QUERY', 'recordImpressionSampleRate') AS float) != 'Infinity'
GROUP BY
    dt,
    parse_url(url, 'QUERY', 'campaign'),
    parse_url(url, 'QUERY', 'banner'),
    parse_url(url, 'QUERY', 'project'),
    parse_url(url, 'QUERY', 'uselang'),
    parse_url(url, 'QUERY', 'bucket'),
    parse_url(url, 'QUERY', 'anonymous') = 'true',
    parse_url(url, 'QUERY', 'statusCode'),
    parse_url(url, 'QUERY', 'country'),
    geocoded_data['country_code'] = parse_url(url, 'QUERY', 'country'),
    geocoded_data['subdivision'],
    parse_url(url, 'QUERY', 'device'),
    cast(parse_url(url, 'QUERY', 'recordImpressionSampleRate') AS float)
;
