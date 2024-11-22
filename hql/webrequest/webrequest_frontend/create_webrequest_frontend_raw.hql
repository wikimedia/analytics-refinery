-- Creates table statement for raw webrequest_frontend table.
--
-- Parameters:
--     database: should be wmf_raw
--
-- Usage
--     spark3-sql -f create_webrequest_frontend_raw_table.hql \
--       --database user1 \
--       -d table=webrequest_frontend \
--       -d location=hdfs://analytics-hadoop/wmf/data/raw/webrequest_frontend
--

CREATE EXTERNAL TABLE IF NOT EXISTS `${table}` (
    `accept`                    string  COMMENT 'Accept header of request',
    `accept_language`           string  COMMENT 'Accept-Language header of request',
    `backend`                   string  COMMENT 'Server HTTP response header',
    `cache_status`              string  COMMENT 'Cache status',
    `content_type`              string  COMMENT 'Content-Type header of response',
    `dt`                        string  COMMENT 'Timestamp at cache in ISO 8601',
    `hostname`                  string  COMMENT 'Source node hostname',
    `http_method`               string  COMMENT 'HTTP method of request',
    `http_status`               string  COMMENT 'HTTP status of response',
    `ip`                        string  COMMENT 'IP of packet at cache',
    `range`                     string  COMMENT 'Range header of response',
    `referer`                   string  COMMENT 'Referer header of request',
    `response_size`             bigint  COMMENT 'Response size',
    `sequence`                  bigint  COMMENT 'Per host sequence number',
    `server_pid`                string  COMMENT 'ID of the process (currently, haproxy) that handled this request',
    `time_firstbyte`            double  COMMENT 'Time to first byte',
    `tls`                       string  COMMENT 'TLS information of request. Format: key1=val1;key2=val2 format',
    `uri_host`                  string  COMMENT 'Host of request',
    `uri_path`                  string  COMMENT 'Path of request',
    `uri_query`                 string  COMMENT 'Query of request',
    `user_agent`                string  COMMENT 'User-Agent header of request',
    `x_analytics`               string  COMMENT 'X-Analytics header of response. Format: key1=val2;key2=val2',
    `x_cache`                   string  COMMENT 'X-Cache header of response',
    `ch_ua`                     string  COMMENT 'Value of the Sec-CH-UA request header',
    `ch_ua_mobile`              string  COMMENT 'Value of the Sec-CH-UA-Mobile request header',
    `ch_ua_platform`            string  COMMENT 'Value of the Sec-CH-UA-Platform request header',
    `ch_ua_arch`                string  COMMENT 'Value of the Sec-CH-UA-Arch request header',
    `ch_ua_bitness`             string  COMMENT 'Value of the Sec-CH-UA-Bitness request header',
    `ch_ua_full_version_list`   string  COMMENT 'Value of the Sec-CH-UA-Full-Version-List request header',
    `ch_ua_model`               string  COMMENT 'Value of the Sec-CH-UA-Model request header',
    `ch_ua_platform_version`    string  COMMENT 'Value of the Sec-CH-UA-Platform-Version request header'
     )
PARTITIONED BY (
    `webrequest_source` string  COMMENT 'Webrequest haproxykafka source (text, upload)',
    `year`              int     COMMENT 'Unpadded year of request',
    `month`             int     COMMENT 'Unpadded month of request',
    `day`               int     COMMENT 'Unpadded day of request',
    `hour`              int     COMMENT 'Unpadded hour of request')
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION
    '${location}'
;
