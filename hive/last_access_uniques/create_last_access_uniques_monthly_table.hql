-- Creates table statement for WMF-Last-Access cookie base uniques monthly table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_last_access_uniques_monthly_table.hql --database wmf


CREATE EXTERNAL TABLE IF NOT EXISTS `last_access_uniques_monthly`(
    `uri_host`             string  COMMENT 'The lower cased host defining a project (en.wikipedia.org for instance)',
    `country`              string  COMMENT 'Country name of the accessing agents (computed using maxmind GeoIP database)',
    `country_code`         string  COMMENT '2 letter country code',
    `uniques_underestimate` int    COMMENT 'Under estimation of unique devices seen based on last access cookie, and the nocookies header',
    `uniques_offset`       int     COMMENT 'Unique devices offset computed as 1-action sessions without cookies',
    `uniques_estimate`     int     COMMENT 'Estimate of unique devices seen as uniques_underestimate plus offset'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of requests',
    `month`             int     COMMENT 'Unpadded month of requests'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/last_access_uniques/monthly'
;
