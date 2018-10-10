-- Creates table statement for refined webrequest table.
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
--     hive -f create_webrequest_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `webrequest`(
    `hostname`          string  COMMENT 'Source node hostname',
    `sequence`          bigint  COMMENT 'Per host sequence number',
    `dt`                string  COMMENT 'Timestame at cache in ISO 8601',
    `time_firstbyte`    double  COMMENT 'Time to first byte',
    `ip`                string  COMMENT 'IP of packet at cache',
    `cache_status`      string  COMMENT 'Cache status',
    `http_status`       string  COMMENT 'HTTP status of response',
    `response_size`     bigint  COMMENT 'Response size',
    `http_method`       string  COMMENT 'HTTP method of request',
    `uri_host`          string  COMMENT 'Host of request',
    `uri_path`          string  COMMENT 'Path of request',
    `uri_query`         string  COMMENT 'Query of request',
    `content_type`      string  COMMENT 'Content-Type header of response',
    `referer`           string  COMMENT 'Referer header of request',
    `x_forwarded_for`   string  COMMENT 'X-Forwarded-For header of request (deprecated)',
    `user_agent`        string  COMMENT 'User-Agent header of request',
    `accept_language`   string  COMMENT 'Accept-Language header of request',
    `x_analytics`       string  COMMENT 'X-Analytics header of response',
    `range`             string  COMMENT 'Range header of response',
    `is_pageview`       boolean COMMENT 'Indicates if this record was marked as a pageview during refinement',
    `record_version`    string  COMMENT 'Keeps track of changes in the table content definition - https://wikitech.wikimedia.org/wiki/Analytics/Data/Webrequest',
    `client_ip`         string  COMMENT 'Client IP - DEPRECATED - Same as IP.',
    `geocoded_data`     map<string, string>  COMMENT 'Geocoded map with continent, country_code, country, city, subdivision, postal_code, latitude, longitude, timezone keys and associated values.',
    -- Waiting for x_cache format to change before parsing into a map
    `x_cache`           string  COMMENT 'X-Cache header of response',
    -- Next two fields are to replace original ua and x_analytics ones.
    -- However such schema modification implies backward incompatibility.
    -- We will replace once we feel confident enough that 'every' backward incompatible change is done.
    `user_agent_map`    map<string, string>  COMMENT 'User-agent map with browser_family, browser_major, device_family, os_family, os_major, os_minor and wmf_app_version keys and associated values',
    `x_analytics_map`   map<string, string>  COMMENT 'X_analytics map view of the x_analytics field',
    `ts`                timestamp            COMMENT 'Unix timestamp in milliseconds extracted from dt',
    `access_method`     string  COMMENT 'Method used to access the site (mobile app|mobile web|desktop)',
    `agent_type`        string  COMMENT 'Categorise the agent making the webrequest as either user or spider (automatas to be added).',
    `is_zero`           boolean COMMENT 'Indicates if the webrequest is accessed through a zero provider',
    `referer_class`     string  COMMENT 'Indicates if a referer is internal, external or unknown.',
    `normalized_host`   struct<project_class: string, project:string, qualifiers: array<string>, tld: String, project_family: string>  COMMENT 'struct containing project_family (such as wikipedia or wikidata for instance), project (such as en or commons), qualifiers (a list of in-between values, such as m and/or zero) and tld (org most often)',
    `pageview_info`     map<string, string>  COMMENT 'map containing project, language_variant and page_title values only when is_pageview = TRUE.',
    `page_id`           bigint  COMMENT 'MediaWiki page_id for this page title. For redirects this could be the page_id of the redirect or the page_id of the target. This may not always be set, even if the page is actually a pageview.',
    `namespace_id`      int     COMMENT 'MediaWiki namespace_id for this page title. This may not always be set, even if the page is actually a pageview.',
    `tags`              array<string> COMMENT 'List containing tags qualifying the request, ex: [portal, wikidata]. Will be used to split webrequest into smaller subsets.',
    `isp_data`          map<string, string>  COMMENT 'Internet Service Provider data in a map with keys isp, organization, autonomous_system_organization and autonomous_system_number',
    `accept`            string  COMMENT 'Accept header of request'
)
PARTITIONED BY (
    `webrequest_source` string  COMMENT 'Source cluster',
    `year`              int     COMMENT 'Unpadded year of request',
    `month`             int     COMMENT 'Unpadded month of request',
    `day`               int     COMMENT 'Unpadded day of request',
    `hour`              int     COMMENT 'Unpadded hour of request'
)
CLUSTERED BY(hostname, sequence) INTO 64 BUCKETS
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/wmf/webrequest'
;

