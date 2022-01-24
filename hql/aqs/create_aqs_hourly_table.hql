-- Creates table statement for aqs_hourly  table.
--
-- NOTE:  When choosing partition field types,
-- one should take into consideration Hive's
-- insistence on storing partition values
-- as strings.  See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png
-- and
-- http://bots.wmflabs.org/~wm-bot/logs/%23wikimedia-analytics/20140721.txt
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_aqs_hourly_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `aqs_hourly`(
    `cache_status`      string  COMMENT 'Cache status',
    `http_status`       string  COMMENT 'HTTP status of response',
    `http_method`       string  COMMENT 'HTTP method of request',
    `response_size`     bigint  COMMENT 'Response size',
    `uri_host`          string  COMMENT 'Host of request',
    `uri_path`          string  COMMENT 'Path of request',
    `request_count`     bigint  COMMENT 'Number of requests'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of request',
    `month`             int     COMMENT 'Unpadded month of request',
    `day`               int     COMMENT 'Unpadded day of request',
    `hour`              int     COMMENT 'Unpadded hour of request'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/aqs/hourly'
;
