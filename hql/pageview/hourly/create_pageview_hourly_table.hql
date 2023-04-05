-- Creates table statement for hourly aggregated pageview table.
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
--     spark3-sql -f create_pageview_hourly_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `pageview_hourly`(
    `project`           string  COMMENT 'Project name from requests hostname',
    `language_variant`  string  COMMENT 'Language variant from requests path (not set if present in project name)',
    `page_title`        string  COMMENT 'Page Title from requests path and query',
    `access_method`     string  COMMENT 'Method used to access the pages, can be desktop, mobile web, or mobile app',
    `zero_carrier`      string  COMMENT 'NULL as zero program is over',
    `agent_type`        string  COMMENT 'Agent accessing the pages, can be spider, user or automated (see https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews/Automated)',
    `referer_class`     string  COMMENT 'Can be none (null, empty or \'-\'), unknown (domain extraction failed), internal (domain is a wikimedia project), external (search engine) (domain is one of google, yahoo, bing, yandex, baidu, duckduckgo), external (any other)',
    `continent`         string  COMMENT 'Continent of the accessing agents (computed using maxmind GeoIP database)',
    `country_code`      string  COMMENT 'Country iso code of the accessing agents (computed using maxmind GeoIP database)',
    `country`           string  COMMENT 'Country (text) of the accessing agents (computed using maxmind GeoIP database)',
    `subdivision`       string  COMMENT 'Subdivision of the accessing agents (computed using maxmind GeoIP database)',
    `city`              string  COMMENT 'City iso code of the accessing agents (computed using maxmind GeoIP database)',
    `user_agent_map`    map<string, string>  COMMENT 'User-agent map with device_family, browser_family, browser_major, os_family, os_major, os_minor and wmf_app_version keys and associated values',
    `record_version`    string  COMMENT 'Keeps track of changes in the table content definition - https://wikitech.wikimedia.org/wiki/Analytics/Data/Pageview_hourly',
    `view_count`        bigint  COMMENT 'number of pageviews',
    `page_id`           bigint  COMMENT 'MediaWiki page_id for this page title. For redirects this could be the page_id of the redirect or the page_id of the target. This may not always be set, even if the page is actually a pageview.',
    `namespace_id`      int     COMMENT 'MediaWiki namespace_id for this page title. This may not always be set, even if the page is actually a pageview.',
    `referer_name`      string  COMMENT 'Name of referer when referer class is external(search engine) or external(media sites)'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of pageviews',
    `month`             int     COMMENT 'Unpadded month of pageviews',
    `day`               int     COMMENT 'Unpadded day of pageviews',
    `hour`              int     COMMENT 'Unpadded hour of pageviews'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/pageview/hourly'
;
