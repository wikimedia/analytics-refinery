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

CREATE EXTERNAL TABLE IF NOT EXISTS `webrequest_sampled` (
    `accept`                  string  COMMENT 'Accept header of request',
    `accept_language`         string  COMMENT 'Accept-Language header of request',
    `as_number`               bigint  COMMENT 'Autonomous-System number retrieved from IP with MaxMind',
    `authorization`           string  COMMENT '',
    `backend`                 string  COMMENT '',
    `cache_status`            string  COMMENT 'Cache status',
    `content_type`            string  COMMENT 'Content-Type header of response',
    `continent`               string  COMMENT 'Continent retrieved from IP with MaxMind',
    `country_code`            string  COMMENT '2 letter country-code retrieved from IP with MaxMind',
    `dt`                      string  COMMENT 'Timestamp at cache in ISO 8601',
    `hostname`                string  COMMENT 'Source node hostname',
    `http_method`             string  COMMENT 'HTTP method of request',
    `http_status`             string  COMMENT 'HTTP status of response',
    `https`                   string  COMMENT '1 if the request uses HTTPS',
    `ip`                      string  COMMENT 'IP of packet at cache',
    `is_debug`                string  COMMENT '1 if the request is in debug mode',
    `is_from_public_cloud`    string  COMMENT '1 if the request is made from a public cloud',
    `is_pageview`             string  COMMENT '1 if the request is considered a wnf pageview',
    `isp`                     string  COMMENT 'ISP retrieved from IP with MaxMind',
    `ja3n`                    string  COMMENT '',
    `ja4h`                    string  COMMENT '',
    `nocookies`               string  COMMENT '1 if the request did not have a cookie',
    `range`                   string  COMMENT 'Range header of response',
    `referer`                 string  COMMENT 'Referer header of request',
    `requestctl`              string  COMMENT 'Comma spearated list of requestctl matching the request',
    `res_proxy`               string  COMMENT '',
    `response_size`           bigint  COMMENT 'Response size',
    `sequence`                bigint  COMMENT 'Per host sequence number',
    `server_pid`              string  COMMENT 'ID of the process (currently, haproxy) that handled this request',
    `termination_state`       string  COMMENT 'HAProxy session states at disconnection / anomalous session termination states. See https://wikitech.wikimedia.org/wiki/HAProxy/session_states',
    `time_firstbyte`          double  COMMENT 'Time to first byte',
    `tls_auth`                string  COMMENT 'TLS ',
    `tls_cipher`              string  COMMENT 'TLS ',
    `tls_key_exchange`        string  COMMENT 'TLS ',
    `tls_sess`                string  COMMENT 'TLS ',
    `tls_version`             string  COMMENT 'TLS version of the request',
    `uri_host`                string  COMMENT 'Host of request',
    `uri_path`                string  COMMENT 'Path of request',
    `uri_query`               string  COMMENT 'Query of request',
    `user_agent`              string  COMMENT 'User-Agent header of request',
    `webrequest_source`       string  COMMENT 'Source cluster',
    `wmfuniq_days`            string  COMMENT '',
    `wmfuniq_freq`            string  COMMENT '',
    `wmfuniq_weeks`           string  COMMENT '',
    `x_cache`                 string  COMMENT 'Cache path of request',
    `x_is_browser`            string  COMMENT ''
) PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of request',
    `month`             int     COMMENT 'Unpadded month of request',
    `day`               int     COMMENT 'Unpadded day of request',
    `hour`              int     COMMENT 'Unpadded hour of request')
ROW FORMAT SERDE
    'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION
    'hdfs://analytics-hadoop/wmf/data/raw/webrequest_sampled/webrequest_sampled'
;
