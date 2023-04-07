-- Creates table statement for raw webrequest table.
--
-- NOTE:  When choosing partition field types, one should take into consideration Hive's insistence on storing
-- partition values as strings.  See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png
--
-- Parameters:
--     database: should be wmf_raw
--
-- Usage
--     spark3-sql -f create_webrequest_raw_table.hql \
--       --database user1
--

CREATE EXTERNAL TABLE IF NOT EXISTS `webrequest` (
    `hostname`                string  COMMENT 'Source node hostname',
    `sequence`                bigint  COMMENT 'Per host sequence number',
    `dt`                      string  COMMENT 'Timestamp at cache in ISO 8601',
    `time_firstbyte`          double  COMMENT 'Time to first byte',
    `ip`                      string  COMMENT 'IP of packet at cache',
    `cache_status`            string  COMMENT 'Cache status',
    `http_status`             string  COMMENT 'HTTP status of response',
    `response_size`           bigint  COMMENT 'Response size',
    `http_method`             string  COMMENT 'HTTP method of request',
    `uri_host`                string  COMMENT 'Host of request',
    `uri_path`                string  COMMENT 'Path of request',
    `uri_query`               string  COMMENT 'Query of request',
    `content_type`            string  COMMENT 'Content-Type header of response',
    `referer`                 string  COMMENT 'Referer header of request',
    `x_forwarded_for`         string  COMMENT 'X-Forwarded-For header of request (deprecated)',
    `user_agent`              string  COMMENT 'User-Agent header of request',
    `accept_language`         string  COMMENT 'Accept-Language header of request',
    `x_analytics`             string  COMMENT 'X-Analytics header of response',
    `range`                   string  COMMENT 'Range header of response',
    `x_cache`                 string  COMMENT 'Cache path of request',
    `accept`                  string  COMMENT 'Accept header of request',
    `tls`                     string  COMMENT 'TLS information of request',
    `ch_ua`                   string  COMMENT 'Value of the Sec-CH-UA request header',
    `ch_ua_mobile`            string  COMMENT 'Value of the Sec-CH-UA-Mobile request header',
    `ch_ua_platform`          string  COMMENT 'Value of the Sec-CH-UA-Platform request header',
    `ch_ua_arch`              string  COMMENT 'Value of the Sec-CH-UA-Arch request header',
    `ch_ua_bitness`           string  COMMENT 'Value of the Sec-CH-UA-Bitness request header',
    `ch_ua_full_version_list` string  COMMENT 'Value of the Sec-CH-UA-Full-Version-List request header',
    `ch_ua_model`             string  COMMENT 'Value of the Sec-CH-UA-Model request header',
    `ch_ua_platform_version`  string  COMMENT 'Value of the Sec-CH-UA-Platform-Version request header'
) PARTITIONED BY (
    `webrequest_source` string  COMMENT 'Source cluster',
    `year`              int     COMMENT 'Unpadded year of request',
    `month`             int     COMMENT 'Unpadded month of request',
    `day`               int     COMMENT 'Unpadded day of request',
    `hour`              int     COMMENT 'Unpadded hour of request')
ROW FORMAT SERDE
    'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION
    'hdfs://analytics-hadoop/wmf/data/raw/webrequest'
;
