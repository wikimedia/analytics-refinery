--
-- Creates a table to store the top N non-compliant User-Agents by request volume,
-- partitioned daily and segmented by webrequest source, compliance status, IP provenance,
-- agent type flags, and HTTP status code.
--
-- Parameters:
--     table_name      Fully qualified name of the table to create.
--     base_directory  HDFS path to use as the table's base location.
--
-- Usage:
--     spark3-sql -f create_user_agent_top_non_compliant_daily_table.hql \
--         -d table_name=wmf_traffic.user_agent_top_non_compliant_daily \
--         -d base_directory=/wmf/data/wmf_traffic/user_agent_top_non_compliant_daily
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
    `webrequest_source`        string          COMMENT 'Ranking partition: source of webrequest logs, text or upload',
    `compliance_status`        string          COMMENT 'Ranking partition: non_compliant or non_compliant_placeholder',
    `ip_provenance`            string          COMMENT 'Ranking partition: wikimedia, wikimedia_labs, or internet',
    `is_instant_commons_agent` boolean         COMMENT 'Ranking partition: whether User-Agent is related to (Quick) Instant Commons',
    `is_foreign_api_agent`     boolean         COMMENT 'Ranking partition: whether User-Agent is related to externally hosted MW instances',
    `is_wikipedia_app_agent`   boolean         COMMENT 'Ranking partition: whether User-Agent is related to Wikipedia Apps',
    `is_api_request`           boolean         COMMENT 'Ranking partition: whether the request is an API request',
    `http_status`              int             COMMENT 'Ranking partition: HTTP response status code',
    `rank`                     int             COMMENT 'Rank of the user agent by request volume within the ranking partition',
    `user_agent`               string          COMMENT 'User-Agent for the criteria',
    `request_count`            bigint          COMMENT 'Number of requests for this user agent within the ranking partition',
    `day`                      date            COMMENT 'The day for which the metric is computed over'
)
USING ICEBERG
PARTITIONED BY (months(`day`))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION '${base_directory}'
;
