-- Creates table statement for raw webrequest table.
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
--     hive -f create_webrequest_raw_table.hql \
--         --database wmf_raw
--

ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS `webrequest` (
    `hostname`          string  COMMENT 'Source node hostname',
    `sequence`          bigint  COMMENT 'Per host sequence number',
    `dt`                string  COMMENT 'Timestame at cache in ISO 8601',
    `time_firstbyte`    double  COMMENT 'Time to first byte',
    `ip`                string  COMMENT 'IP of packet at cache',
    `cache_status`      string  COMMENT 'Cache status',
    `http_status`       string  COMMENT 'HTTP status of response',
    `response_size`     bigint  COMMENT 'Response size',
    `http_method`       string  COMMENT 'HTTP method of request',
    `uri_host`          string  COMMENT 'Host of request',
    `uri_path`          string  COMMENT 'Path of request',
    `uri_query`         string  COMMENT 'Query of request',
    `content_type`      string  COMMENT 'Content-Type header of response',
    `referer`           string  COMMENT 'Referer header of request',
    `x_forwarded_for`   string  COMMENT 'X-Forwarded-For header of request (deprecated)',
    `user_agent`        string  COMMENT 'User-Agent header of request',
    `accept_language`   string  COMMENT 'Accept-Language header of request',
    `x_analytics`       string  COMMENT 'X-Analytics header of response',
    `range`             string  COMMENT 'Range header of response',
    `x_cache`           string  COMMENT 'Cache path of request',
    `accept`            string  COMMENT 'Accept header of request')
PARTITIONED BY (
    `webrequest_source` string  COMMENT 'Source cluster',
    `year`              int     COMMENT 'Unpadded year of request',
    `month`             int     COMMENT 'Unpadded month of request',
    `day`               int     COMMENT 'Unpadded day of request',
    `hour`              int     COMMENT 'Unpadded hour of request')
ROW FORMAT SERDE
    'org.apache.hive.hcatalog.data.JsonSerDe'
-- We only care about the INPUTFORMAT, not the OUTPUTFORMAT. But
-- Hive's syntax does not allow to specify one without the
-- other. Hence, we give both and use a default for the OUTPUTFORMAT.
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.SequenceFileInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    'hdfs://analytics-hadoop/wmf/data/raw/webrequest'
;
