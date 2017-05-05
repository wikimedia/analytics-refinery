-- Creates table statement for unique devices project-wide daily table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_unique_devices_project_wide_daily_table.hql --database wmf


CREATE EXTERNAL TABLE IF NOT EXISTS `unique_devices_project_wide_daily`(
    `project`              string  COMMENT 'The lower cased project (wikipedia for instance)',
    `country`              string  COMMENT 'Country name of the accessing agents (computed using maxmind GeoIP database)',
    `country_code`         string  COMMENT '2 letter country code',
    `uniques_underestimate` int    COMMENT 'Under estimation of unique devices seen based on last-access-global cookie, and the nocookies header',
    `uniques_offset`        int    COMMENT 'Unique devices offset computed as fresh sessions (only 1 action without last-access-global cookie)',
    `uniques_estimate`      int    COMMENT 'Estimate of unique devices seen as uniques_underestimate plus offset'
)
PARTITIONED BY (
    `year`                  int    COMMENT 'Unpadded year of requests',
    `month`                 int    COMMENT 'Unpadded month of requests',
    `day`                   int    COMMENT 'Unpadded day of requests'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/unique_devices/project_wide/daily'
;
