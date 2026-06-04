--
-- Creates a table to store refined and enriched API requests from webrequest, partitioned hourly.
--
-- Parameters:
--     table_name      Fully qualified name of the table to create.
--     base_directory  HDFS path to use as the table's base location.
--
-- Usage:
--     spark3-sql -f create_api_requests_refined_table.hql \
--         -d table_name=wmf_traffic.api_requests_refined \
--         -d base_directory=/wmf/data/wmf_traffic/api_requests_refined
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
    `hostname`                 string                                                                                          COMMENT 'Source node hostname',
    `sequence`                 bigint                                                                                          COMMENT 'Per host sequence number',
    `ts`                       timestamp                                                                                       COMMENT 'Unix timestamp in milliseconds extracted from dt',
    `ip`                       string                                                                                          COMMENT 'IP of packet at cache',
    `user_agent`               string                                                                                          COMMENT 'User-Agent header of request',
    `user_agent_map`           map<string, string>                                                                             COMMENT 'User-agent map with browser_family, browser_major, device_family, os_family, os_major, os_minor and wmf_app_version keys',
    `cache_status_normalized`  string                                                                                          COMMENT 'Simplified cache status: hit, int, pass, miss, or unknown',
    `http_status`              int                                                                                             COMMENT 'HTTP response status code',
    `uri_host`                 string                                                                                          COMMENT 'Host of request',
    `uri_path`                 string                                                                                          COMMENT 'Path of request',
    `uri_query`                string                                                                                          COMMENT 'Query of request',
    `normalized_host`          struct<project_class:string, project:string, qualifiers:array<string>, tld:string, project_family:string>  COMMENT 'Parsed host struct with project_family, project, qualifiers, and tld',
    `referer_data`             struct<referer_class:string, referer_name:string>                                               COMMENT 'Parsed referer struct with referer_class and referer_name',
    `accept_language`          string                                                                                          COMMENT 'Accept-Language header of request',
    `geocoded_data`            map<string, string>                                                                             COMMENT 'Geocoded map with continent, country_code, country, city, subdivision, postal_code, latitude, longitude, timezone',
    `isp_data`                 map<string, string>                                                                             COMMENT 'ISP data map with isp, organization, autonomous_system_organization and autonomous_system_number',
    `is_api_request`           boolean                                                                                         COMMENT 'Always true for records in this table',
    `is_api_gateway_routed`    boolean                                                                                         COMMENT 'Indicates whether the request was routed through the Centralised Gateway',
    `is_ats_bypass`            boolean                                                                                         COMMENT 'Indicates whether the request had an ATS BYPASS exception',
    `api_family`               string                                                                                          COMMENT 'API family inferred from uri_host, uri_path, and uri_query',
    `api_resource_type`        string                                                                                          COMMENT 'Resource type of the API endpoint',
    `api_endpoint_version`     string                                                                                          COMMENT 'Version of the API endpoint',
    `agent_type`               string                                                                                          COMMENT 'Categorised agent type: user or spider',
    `ip_provenance`            string                                                                                          COMMENT 'Network origin of the IP of request: wikimedia, wikimedia_labs, or internet',
    `user_agent_compliance`    string                                                                                          COMMENT 'User-Agent compliance level',
    `user_agent_segment`       string                                                                                          COMMENT 'User-Agent segment',
    `x_trusted_request`        string                                                                                          COMMENT 'Trusted request flag from x_analytics_map',
    `ratelimit_class`          string                                                                                          COMMENT 'Rate limit class from x_analytics_map',
    `x_is_browser_score`       double                                                                                          COMMENT 'Browser indicator score from x_analytics_map',
    `auth_type`                string                                                                                          COMMENT 'Authentication type from x_analytics_map',
    `has_cookies`              boolean                                                                                         COMMENT 'Indicates whether the request included cookies',
    `requestctl`               string                                                                                          COMMENT 'Request control value from x_analytics_map',
    `hap_known_client_action`  string                                                                                          COMMENT 'HAProxy known client action extracted from requestctl',
    `hap_known_client_name`    string                                                                                          COMMENT 'HAProxy known client name extracted from requestctl',
    `res_proxy`                string                                                                                          COMMENT 'Proxy info from x_analytics_map',
    `response_size_bytes`      bigint                                                                                          COMMENT 'Response size in bytes',
    `time_firstbyte_sec`       double                                                                                          COMMENT 'Time to first byte in seconds',
    `hour`                     timestamp                                                                                       COMMENT 'The hour during which the request was received'
)
USING ICEBERG
PARTITIONED BY (hours(`hour`))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION '${base_directory}'
;
