-- Creates table statement for aqs_hourly table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_aqs_hourly_table_iceberg.hql    \
--     --database wmf_traffic                               \
--     -d location=/wmf/data/wmf_traffic/aqs/hourly

CREATE EXTERNAL TABLE IF NOT EXISTS `aqs_hourly`(
    `cache_status`      string     COMMENT 'Cache status',
    `http_status`       string     COMMENT 'HTTP status of response',
    `http_method`       string     COMMENT 'HTTP method of request',
    `response_size`     bigint     COMMENT 'Response size',
    `uri_host`          string     COMMENT 'Host of request',
    `uri_path`          string     COMMENT 'Path of request',
    `request_count`     bigint     COMMENT 'Number of requests',
    `hour`              timestamp  COMMENT 'The hour for which the metric is computed over. Aggregated over minute 00 to 59.
    Ex: Count rows from first hour of the month: SELECT count(1) FROM wmf_traffic.aqs_hourly WHERE hour = \'2023-12-01 00:00:00\''
)
USING ICEBERG
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'lz4'
)
PARTITIONED BY (days(hour))
LOCATION '${location}'
;
