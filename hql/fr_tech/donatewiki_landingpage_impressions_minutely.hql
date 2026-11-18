CREATE TABLE IF NOT EXISTS ${destination_table} (
    `dt`                     timestamp,
    `squid_hostname`         string,
    `squid_sequence_number`  bigint,
    `agent_type`             string,
    `http_status`            string,
    `uri_host`               string,
    `uri_path`               string,
    `uri_query`              string,
    `subdomain`              string,
    `country`                string,
    `country_code`           string,
    `utm_source`             string,
    `utm_medium`             string,
    `utm_campaign`           string,
    `utm_key`                string
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


WITH filtered_data AS (
    SELECT
        dt,
        server.hostname          AS squid_hostname,
        server.sequence_number   AS squid_sequence_number,
        agent_type,
        request.http_status      AS http_status,
        target.uri_host          AS uri_host,
        target.uri_path          AS uri_path,
        target.uri_query         AS uri_query,
        target.subdomain         AS subdomain,
        geo.country              AS country,
        geo.country_code         AS country_code,
        -- parse_url needs a full URL
        concat('http://bla.org/woo', target.uri_query) AS url
    FROM
        ${source_table}
    WHERE
        -- make timestamps lets us use iceberg partition pruning
        dt >= make_timestamp(${year}, ${month}, ${day}, ${hour}, 0, 0)
        AND dt <  make_timestamp(${year}, ${month}, ${day}, ${hour}, 0, 0) + INTERVAL 1 HOUR
        AND request_type = 'other'
        AND target.uri_host = 'donate.wikimedia.org'
        -- exclude rows with no utm params
        AND target.uri_query LIKE '%utm%'
        -- only include LP impression rows that include title uri param to match pgehres filter 
        AND substr(target.uri_query, 1, 3) = '?ti'
)


INSERT OVERWRITE TABLE ${destination_table}
PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})

SELECT /*+ REPARTITION(${coalesce_partitions}) */
    dt,
    squid_hostname,
    squid_sequence_number,
    agent_type,
    http_status,
    uri_host,
    uri_path,
    uri_query,
    subdomain,
    country,
    country_code,
    parse_url(url, 'QUERY', 'utm_source')   AS utm_source,
    parse_url(url, 'QUERY', 'utm_medium')   AS utm_medium,
    parse_url(url, 'QUERY', 'utm_campaign') AS utm_campaign,
    parse_url(url, 'QUERY', 'utm_key')      AS utm_key
FROM
    filtered_data
;