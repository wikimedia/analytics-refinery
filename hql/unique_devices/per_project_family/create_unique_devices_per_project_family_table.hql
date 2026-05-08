-- Create table statement for unique devices per project family table.
-- It can be used to create tables for any granularity.
--
-- Usage
--     spark3-sql -f create_unique_devices_per_project_family_table.hql \
--         --database wmf_readership \
--         -d table_name=unique_devices_per_project_family_daily \
--         -d location=/wmf/data/wmf_readership/unique_devices/per_project_family/daily

CREATE EXTERNAL TABLE IF NOT EXISTS `${table_name}`(
    `project_family`         string  COMMENT 'The lower cased project-family (wikipedia for instance)',
    `country`                string  COMMENT 'Country name of the accessing agents (computed using maxmind GeoIP database)',
    `country_code`           string  COMMENT '2 letter country code',
    `uniques_underestimate`  int     COMMENT 'Under estimation of unique devices seen based on last-access-global cookie, and the nocookies header',
    `uniques_offset`         int     COMMENT 'Unique devices offset computed as fresh sessions (only 1 action without last-access-global cookie)',
    `uniques_estimate`       int     COMMENT 'Estimate of unique devices seen as uniques_underestimate plus offset',
    `day`                    date    COMMENT 'The start date for which the unique-devices metrics are computed'
)
USING ICEBERG
-- The data is small enough to partition it by year (less than 10Mb / year)
PARTITIONED BY (years(day))
LOCATION '${location}'
;
