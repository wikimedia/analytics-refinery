-- Create table statement for hourly aggregated virtualpageviews.
--
-- NOTE:  When choosing partition field types, one should take into consi-
-- deration Hive's insistence on storing partition values as strings. See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png and
-- http://bots.wmflabs.org/~wm-bot/logs/%23wikimedia-analytics/20140721.txt
--
-- Usage
--     hive -f create_virtualpageview_hourly_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `virtualpageview_hourly`(
    `project`             string  COMMENT 'Project name from hostname',
    `language_variant`    string  COMMENT 'Language variant from path (not set if present in project name)',
    `page_title`          string  COMMENT 'Page title from popup preview (canonical)',
    `access_method`       string  COMMENT 'Always desktop (virtualpageviews are a desktop only feature for now)',
    `agent_type`          string  COMMENT 'Agent accessing the pages, can be spider or user',
    `referer_class`       string  COMMENT 'Always internal (virtualpageviews are always shown in wiki pages)',
    `continent`           string  COMMENT 'Continent of the accessing agents (maxmind GeoIP database)',
    `country_code`        string  COMMENT 'Country iso code of the accessing agents (maxmind GeoIP database)',
    `country`             string  COMMENT 'Country (text) of the accessing agents (maxmind GeoIP database)',
    `subdivision`         string  COMMENT 'Subdivision of the accessing agents (maxmind GeoIP database)',
    `city`                string  COMMENT 'City iso code of the accessing agents (maxmind GeoIP database)',
    `user_agent_map`      map<string, string>  COMMENT 'User-agent map with device_family, browser_family, browser_major, os_family, os_major, os_minor and wmf_app_version keys and associated values',
    `record_version`      string  COMMENT 'Keeps track of changes in the table content definition - https://wikitech.wikimedia.org/wiki/Analytics/Data/virtualpageview_hourly',
    `view_count`          bigint  COMMENT 'Number of virtualpageviews of the corresponding bucket',
    `page_id`             bigint  COMMENT 'Page ID from popup preview',
    `namespace_id`        int     COMMENT 'Namespace ID from popup preview',
    `source_page_title`   string  COMMENT 'Page title from source page (canonical)',
    `source_page_id`      bigint  COMMENT 'Page ID from source page',
    `source_namespace_id` int     COMMENT 'Namespace ID from source page'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year',
    `month`             int     COMMENT 'Unpadded month',
    `day`               int     COMMENT 'Unpadded day',
    `hour`              int     COMMENT 'Unpadded hour'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/virtualpageview/hourly'
;
