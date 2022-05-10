-- Creates daily table for webrequests to officewiki
--
--
-- Usage
--     hive -f create_officewiki_webrequest_daily_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `officewiki_webrequest_daily` (
    `uri_path`                string  COMMENT 'Path of request',
    `actor_signature`         string  COMMENT 'standard actor signature',
    `http_status`             string  COMMENT 'HTTP status of response'
)
PARTITIONED BY (
    `year`                int    COMMENT 'Unpadded year',
    `month`               int    COMMENT 'Unpadded month',
    `day`                 int    COMMENT 'Unpadded day'
)
STORED AS PARQUETFILE
LOCATION '/wmf/data/wmf/webrequest_officewiki/daily'
;
