-- Create table statement for raw webrequest table.
--
-- NOTE:  When choosing partition field types,
-- one should take into consideration Hive's
-- insistence on storing partition values
-- as strings.  See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png
-- and
-- http://bots.wmflabs.org/~wm-bot/logs/%23wikimedia-analytics/20140721.txt
--
CREATE EXTERNAL TABLE IF NOT EXISTS `webrequest` (
    `hostname`          string,
    `sequence`          bigint,
    `dt`                string,
    `time_firstbyte`    float,
    `ip`                string,
    `cache_status`      string,
    `http_status`       string,
    `response_size`     int,
    `http_method`       string,
    `uri_host`          string,
    `uri_path`          string,
    `uri_query`         string,
    `content_type`      string,
    `referer`           string,
    `x_forwarded_for`   string,
    `user_agent`        string,
    `accept_language`   string,
    `x_analytics`       string)
PARTITIONED BY (
    `webrequest_source` string,
    `year`              int,
    `month`             int,
    `day`               int,
    `hour`              int)
ROW FORMAT SERDE
    'org.apache.hcatalog.data.JsonSerDe'
-- We only care about the INPUTFORMAT, not the OUTPUTFORMAT. But
-- Hive's syntax does not allow to specify one without the
-- other. Hence, we give both and use a default for the OUTPUTFORMAT.
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.SequenceFileInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '/wmf/data/raw/webrequest'
;
