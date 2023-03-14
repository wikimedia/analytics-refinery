-- Creates table statement for hourly extracted pageview-actor table.
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
--     hive -f create_pageview_actor_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `pageview_actor`(
    -- Kept to facilitate joining back to webrequest if needed
    `hostname`          string  COMMENT 'Source node hostname',
    `sequence`          bigint  COMMENT 'Per host sequence number',

    `dt`                string  COMMENT 'Timestame at cache in ISO 8601',
    `time_firstbyte`    double  COMMENT 'Time to first byte',

    `ip`                string  COMMENT 'IP of packet at cache',
    -- Kept as pageview and redirect-to-pageview can have different http-status than 200
    `http_status`       string  COMMENT 'HTTP status of response',
    `response_size`     bigint  COMMENT 'Response size',
    `uri_host`          string  COMMENT 'Host of request',
    `uri_path`          string  COMMENT 'Path of request',
    `uri_query`         string  COMMENT 'Query of request',
    `content_type`      string  COMMENT 'Content-Type header of response',
    `referer`           string  COMMENT 'Referer header of request',

    -- Keeping raw user-agent, might be useful
    `user_agent`        string  COMMENT 'User-Agent header of request',
    `accept_language`   string  COMMENT 'Accept-Language header of request',

    -- Addition of precomputed is_redirect_to_pageview
    `is_pageview`       boolean COMMENT 'Indicates if this record was marked as a pageview during refinement',
    `is_redirect_to_pageview`  boolean COMMENT 'Indicates if this record was marked as a redirect to a pageview during extraction (needed for unique-devices-per-project-family)',

    `geocoded_data`     map<string, string>  COMMENT 'Geocoded map with continent, country_code, country, city, subdivision, postal_code, latitude, longitude, timezone keys and associated values.',

    `user_agent_map`    map<string, string>  COMMENT 'User-agent map with browser_family, browser_major, device_family, os_family, os_major, os_minor and wmf_app_version keys and associated values',
    `x_analytics_map`   map<string, string>  COMMENT 'X_analytics map view of the x_analytics field',
    `ts`                timestamp            COMMENT 'Unix timestamp in milliseconds extracted from dt',
    `access_method`     string  COMMENT 'Method used to access the site (mobile app|mobile web|desktop)',
    -- Note that automated agent-type is added here in comparison to webrequest
    `agent_type`        string  COMMENT 'Categorise the agent making the webrequest as either user, spider or automated',

    `referer_class`     string  COMMENT 'Indicates if a referer is internal, external or unknown.',
    `normalized_host`   struct<project_class: string, project:string, qualifiers: array<string>, tld: String, project_family: string>  COMMENT 'struct containing project_family (such as wikipedia or wikidata for instance), project (such as en or commons), qualifiers (a list of in-between values, such as m) and tld (org most often)',
    `pageview_info`     map<string, string>  COMMENT 'map containing project, language_variant and page_title values only when is_pageview = TRUE.',
    `page_id`           bigint  COMMENT 'MediaWiki page_id for this page title. For redirects this could be the page_id of the redirect or the page_id of the target. This may not always be set, even if the page is actually a pageview.',
    `namespace_id`      int     COMMENT 'MediaWiki namespace_id for this page title. This may not always be set, even if the page is actually a pageview.',
    `actor_signature`   string  COMMENT 'The actor signature for the record using domain, computed as a hash',
    `actor_signature_per_project_family` string  COMMENT 'The actor signature for the record using project-family, computed as a hash',
    `referer_data`      struct<referer_class:string,referer_name:string> COMMENT 'Struct containing referer_class (indicates if a referer is internal, external, external(media sites), external(search engine) or unknown.) and referer name (name of referer when referer class is external(search engine) or external(media sites))'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of pageviews',
    `month`             int     COMMENT 'Unpadded month of pageviews',
    `day`               int     COMMENT 'Unpadded day of pageviews',
    `hour`              int     COMMENT 'Unpadded hour of pageviews'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/pageview/actor'
;
