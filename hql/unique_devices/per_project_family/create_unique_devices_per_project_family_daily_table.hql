-- Create table statement for unique devices per project-family daily table.
--
-- Usage
--     spark-sql -f create_unique_devices_per_project_family_daily_table.hql \
--         --database wmf \
--         -d location=/wmf/data/wmf/unique_devices/per_project_family/daily


CREATE EXTERNAL TABLE IF NOT EXISTS `unique_devices_per_project_family_daily`(
    `project_family`         string  COMMENT 'The lower cased project-family (wikipedia for instance)',
    `country`                string  COMMENT 'Country name of the accessing agents (computed using maxmind GeoIP database)',
    `country_code`           string  COMMENT '2 letter country code',
    `uniques_underestimate`  int     COMMENT 'Under estimation of unique devices seen based on last-access-global cookie, and the nocookies header',
    `uniques_offset`         int     COMMENT 'Unique devices offset computed as fresh sessions (only 1 action without last-access-global cookie)',
    `uniques_estimate`       int     COMMENT 'Estimate of unique devices seen as uniques_underestimate plus offset'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of requests',
    `month`             int     COMMENT 'Unpadded month of requests',
    `day`               int     COMMENT 'Unpadded day of requests'
)
STORED AS PARQUET
LOCATION '${location}'
;
