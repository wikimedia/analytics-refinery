-- Create table statement for unique devices per domain monthly table.
--
-- Usage
--     spark-sql -f create_unique_devices_per_domain_monthly_table_iceberg.hql \
--         --database wmf_readership \
--         -d location=/wmf/data/wmf_readership/unique_devices/per_domain/monthly


CREATE EXTERNAL TABLE IF NOT EXISTS `unique_devices_per_domain_monthly`(
    `domain`                 string  COMMENT 'The lower cased domain defining a project (en.wikipedia.org for instance)',
    `country`                string  COMMENT 'Country name of the accessing agents (computed using maxmind GeoIP database)',
    `country_code`           string  COMMENT '2 letter country code',
    `uniques_underestimate`  int     COMMENT 'Under estimation of unique devices seen based on last access cookie, and the nocookies header',
    `uniques_offset`         int     COMMENT 'Unique devices offset computed as 1-action sessions without cookies',
    `uniques_estimate`       int     COMMENT 'Estimate of unique devices seen as uniques_underestimate plus offset',
    `day`                    date    COMMENT 'The date for which the unique-devices metrics are computed - only first day of months'
)
USING ICEBERG
-- The data is small enough to partition it by year (less than 20Mb / year)
PARTITIONED BY (years(day))
LOCATION '${location}'
;