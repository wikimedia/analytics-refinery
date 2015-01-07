-- Creates table statement for refined webrequest table.
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
--     hive -f create_webrequest_refinfed_table.hql --database wmf
--
--

CREATE TABLE IF NOT EXISTS `webrequest`(
    `hostname` string COMMENT 'Cache node hostname that served this request',
    `sequence` bigint COMMENT 'Sequence number of request on source cache instance',
    `dt` string COMMENT 'YYYY-MM-DDTHH:mm:ssZ timestamp',
    `time_firstbyte` double COMMENT 'time until the first byte was served',
    `ip` string,
    `cache_status` string,
    `http_status` string,
    `response_size` bigint COMMENT 'Response size in bytes',
    `http_method` string COMMENT 'Request HTTP method',
    `uri_host` string,
    `uri_path` string,
    `uri_query` string,
    `content_type` string COMMENT 'ContentType of response',
    `referer` string,
    `x_forwarded_for` string COMMENT 'X-Forwarded-For header',
    `user_agent` string,
    `accept_language` string COMMENT 'AcceptLanguage header',
    `x_analytics` string COMMENT 'X-Analytics header',
    `range` string COMMENT 'Range field for multipart files',
    `is_pageview` boolean COMMENT 'Indicates if this record was marked as a pageview during refinement'
)
PARTITIONED BY (
    `webrequest_source` string COMMENT 'Source cluster',
    `year` int COMMENT 'Unpadded year of request',
    `month` int COMMENT 'Unpadded month of request',
    `day` int COMMENT 'Unpadded day of request',
    `hour` int COMMENT 'Unpadded hour of request')
CLUSTERED BY(hostname, sequence) INTO 64 BUCKETS
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
    INPUTFORMAT
        'parquet.hive.DeprecatedParquetInputFormat'
    OUTPUTFORMAT
        'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/wmf/data/wmf/webrequest'
;
