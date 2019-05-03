-- Creates table statement for hourly aggregated projectview table.
--
-- NOTE:  When choosing partition field types,
-- one should take into consideration Hive's
-- insistence on storing partition values
-- as strings.  See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png
-- and
-- http://bots.wmflabs.org/~wm-bot/logs/%23wikimedia-analytics/20140721.txt
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_projectview_hourly_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `projectview_hourly`(
    `project`           string  COMMENT 'Project name from requests hostname',
    `access_method`     string  COMMENT 'Method used to access the pages, can be desktop, mobile web, or mobile app',
    `zero_carrier`      string  COMMENT 'NULL as zero program is over',
    `agent_type`        string  COMMENT 'Agent accessing the pages, can be spider or user',
    `referer_class`     string  COMMENT 'Can be internal, external or unknown',
    `continent`         string  COMMENT 'Continent of the accessing agents (computed using maxmind GeoIP database)',
    `country_code`      string  COMMENT 'Country iso code of the accessing agents (computed using maxmind GeoIP database)',
    `record_version`    string  COMMENT 'Keeps track of changes in the table content definition - https://wikitech.wikimedia.org/wiki/Analytics/Data/Projectview_hourly',
    `view_count`        bigint  COMMENT 'number of pageviews'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of projectviews',
    `month`             int     COMMENT 'Unpadded month of projectviews',
    `day`               int     COMMENT 'Unpadded day of projectviews',
    `hour`              int     COMMENT 'Unpadded hour of projectviews'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/projectview/hourly'
;
