--
-- Creates a table to store User-Agent compliance metrics, partitioned hourly.
--
-- Parameters:
--     table_name      Fully qualified name of the table to create.
--     base_directory  HDFS path to use as the table's base location.
--
-- Usage:
--     spark3-sql -f create_user_agent_compliance_hourly_table.hql \
--         -d table_name=wmf_traffic.user_agent_compliance_hourly \
--         -d base_directory=/wmf/data/wmf_traffic/user_agent_compliance_hourly
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
    `webrequest_source`        string          COMMENT 'Source of webrequest logs: text or upload',
    `http_status`              int             COMMENT 'HTTP response status code',
    `cache_status`             string          COMMENT 'Simplified cache status: hit, miss, int, pass',
    `is_api_request`           boolean         COMMENT 'Indicates whether the request is an API request',
    `is_endpoint_index_php`    boolean         COMMENT 'Indicates whether requests are to index.php endpoint',
    `is_endpoint_load_php`     boolean         COMMENT 'Indicates whether requests are to load.php endpoint',
    `is_instant_commons_agent` boolean         COMMENT 'Indicates whether User-Agents related to (Quick) Instant Commons',
    `is_foreign_api_agent`     boolean         COMMENT 'Indicates whether User-Agents related externally hosted MW instances',
    `is_wikipedia_app_agent`   boolean         COMMENT 'Indicates whether User-Agents related to Wikipedia Apps',
    `ip_provenance`            string          COMMENT 'Network origin of the IP of request: wikimedia, wikimedia_labs, or internet',
    `compliance_status`        string          COMMENT 'Indicates the User-Agent compliance of the request: compliant, partially_compliant, non_compliant',
    `contact_method`           array<string>   COMMENT 'Available contact methods for the request: email, url, mw_user',
    `request_count`            bigint          COMMENT 'Number of the requests for the given set of dimensions',
    `response_size_total`      bigint          COMMENT 'Total response size served to requests for the given set of dimensions',
    `ttfb_sec_total`           double          COMMENT 'Total time to first byte in seconds for the given set of dimensions',
    `unique_user_agent_count`  bigint          COMMENT 'Number of unique user agents for the given set of dimensions',
    `hour`                     timestamp       COMMENT 'The hour for which the metric is computed over'
)
USING ICEBERG
PARTITIONED BY (days(`hour`))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION '${base_directory}'
;
