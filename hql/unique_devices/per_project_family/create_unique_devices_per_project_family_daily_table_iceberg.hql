-- Create table statement for unique devices per project-family daily table.
--
-- Usage
--     spark-sql -f create_unique_devices_per_project_family_daily_table_iceberg.hql \
--         --database wmf_readership \
--         -d location=/wmf/data/wmf_readership/unique_devices/per_project_family/daily


CREATE EXTERNAL TABLE IF NOT EXISTS `unique_devices_per_project_family_daily`(
    `project_family`         string  COMMENT 'The lower cased project-family (wikipedia for instance)',
    `country`                string  COMMENT 'Country name of the accessing agents (computed using maxmind GeoIP database)',
    `country_code`           string  COMMENT '2 letter country code',
    `uniques_underestimate`  int     COMMENT 'Under estimation of unique devices seen based on last-access-global cookie, and the nocookies header',
    `uniques_offset`         int     COMMENT 'Unique devices offset computed as fresh sessions (only 1 action without last-access-global cookie)',
    `uniques_estimate`       int     COMMENT 'Estimate of unique devices seen as uniques_underestimate plus offset',
    `day`                    date    COMMENT 'The date for which the unique-devices metrics are computed'
)
USING ICEBERG
-- The data is small enough to partition it by year (less than 10Mb / year)
PARTITIONED BY (years(day))
LOCATION '${location}'
;
