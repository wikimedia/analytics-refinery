-- Extracts one day of formatted minutely banner activity to be loaded to fr tech's minio and druid
--
-- Usage:
--     spark3-sql -f banner_activity_minutely.hql \
--         -d source_table=wmf.webrequest \
--         -d destination_table=wmf_fr_tech.banner_activity_minutely \
--         -d destination_directory=/wmf/data/wmf_fr_tech/banner_activity_minutely \
--         -d coalesce_partitions=1 \
--         -d year=2023 \
--         -d month=1 \
--         -d day=1



CREATE TABLE IF NOT EXISTS ${destination_table} (
    `dt`                             string,
    `campaign`                       string,
    `banner`                         string,
    `project`                        string,
    `uselang`                        string,
    `bucket`                         string,
    `anonymous`                      boolean,
    `status_code`                    string,
    `first_campaign`                 string,
    `first_campaign_status_code`     string,
    `is_campaign_fallback`           boolean,
    `country`                        string,
    `country_matches_geocode`        boolean,
    `region`                         string,
    `device`                         string,
    `sample_rate`                    float,
    `request_count`                  bigint,
    `normalized_request_count`       bigint
)
USING PARQUET
PARTITIONED BY (
    `year`     int,
    `month`    int,
    `day`      int,
    `hour`     int
)
OPTIONS ('compression'='gzip')
LOCATION '${destination_directory}';


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


WITH filtered_data AS (
    SELECT
        CONCAT(SUBSTRING(dt, 0, 17), '00Z') AS dt,
        concat('http://bla.org/woo/', uri_query) AS url,
        geocoded_data,
        from_json(
            reflect('java.net.URLDecoder', 'decode',
                COALESCE(
                    parse_url(concat('http://bla.org/woo/', uri_query), 'QUERY', 'campaignStatuses'),
                ''),
            'utf-8'),
        'array<struct<statusCode:string, campaign:string, bannersCount:int>>')[0] AS first_campaign_status,
        year,
        month,
        day,
        hour
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
PARTITION(year=${year},month=${month},day=${day}, hour)

SELECT
    dt,
    parse_url(url, 'QUERY', 'campaign') AS campaign,
    parse_url(url, 'QUERY', 'banner') AS banner,
    parse_url(url, 'QUERY', 'project') AS project,
    parse_url(url, 'QUERY', 'uselang') AS uselang,
    parse_url(url, 'QUERY', 'bucket') AS bucket,
    parse_url(url, 'QUERY', 'anonymous') = 'true' AS anonymous,
    parse_url(url, 'QUERY', 'statusCode') AS status_code,
    first_campaign_status.campaign AS first_campaign,
    first_campaign_status.statusCode AS first_campaign_status_code,
    (first_campaign_status.campaign <> reflect('java.net.URLDecoder', 'decode', COALESCE(parse_url(url, 'QUERY', 'campaign'), ''), 'utf-8')) AS is_campaign_fallback,
    parse_url(url, 'QUERY', 'country') AS country,
    geocoded_data['country_code'] = parse_url(url, 'QUERY', 'country') AS country_matches_geocode,
    geocoded_data['subdivision'] AS region,
    parse_url(url, 'QUERY', 'device') AS device,
    cast(parse_url(url, 'QUERY', 'recordImpressionSampleRate') AS float) AS sample_rate,
    COUNT(*) AS request_count,
    cast(COUNT(*) / cast(parse_url(url, 'QUERY', 'recordImpressionSampleRate') AS float) AS bigint) AS normalized_request_count,
    hour
FROM
    filtered_data
WHERE
    parse_url(url, 'QUERY', 'debug') = 'false'
    -- sample_rate can be infinity, leading to Druid indexation failing.
    -- We remove those rows from the data
    AND cast(parse_url(url, 'QUERY', 'recordImpressionSampleRate') AS float) != 'Infinity'
GROUP BY
    dt,
    campaign,
    banner,
    project,
    uselang,
    bucket,
    anonymous,
    status_code,
    first_campaign,
    first_campaign_status_code,
    is_campaign_fallback,
    country,
    country_matches_geocode,
    region,
    device,
    sample_rate,
    hour

DISTRIBUTE BY hour
;
