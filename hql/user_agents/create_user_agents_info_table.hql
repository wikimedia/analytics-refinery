--
-- Creates a table to store User-Agent metadata from various traffic sources, partitioned hourly and by source.
-- A User-Agent appearing in multiple sources during an hour will have one record per source.
-- Please specify a source when using the table, to avoid accidental duplicate counts.
--
-- Parameters:
--     table_name      Fully qualified name of the table to create.
--     base_directory  HDFS path to use as the table's base location.
--
-- Usage:
--     spark3-sql -f create_user_agents_info_table.hql \
--         -d table_name=wmf_traffic.user_agents_info \
--         -d base_directory=/wmf/data/wmf_traffic/user_agents_info
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
    `source`                                string             COMMENT 'Source dataset where the User-Agent was seen, e.g. webrequest or dumps',
    `user_agent`                            string             COMMENT 'User-Agent string',
    `parsed_user_agent`                     struct<
                                                browser_family:string,
                                                browser_major:int,
                                                device_family:string,
                                                os_family:string,
                                                os_major:int,
                                                os_minor:int,
                                                wmf_app_version:string
                                            >                  COMMENT 'Parsed User-Agent fields',
    `contact_info`                          map<string,string> COMMENT 'Extracted contact information keyed by type (email, url, mw_user, placeholder_email, placeholder_url)',
    `ua_policy_compliance_level`            string             COMMENT 'Compliance with WMF User-Agent policy: compliant, partially_compliant, non_compliant, non_compliant_placeholder, or browser_like',
    `bot_category`                          string             COMMENT 'Bot taxonomy category for identified bots; generic_bot for bot-like agents without a category; NULL for non-bot like agents',
    `identified_agent`                      string             COMMENT 'Internally identified agent type, e.g. wikipedia_app, instant_commons, mw_foreign_repo',
    `request_count`                         bigint             COMMENT 'Total number of requests from this User-Agent at the source during the hour',
    `cached_requests_proportion`            double             COMMENT 'Proportion of requests served from cache (cache- hit or int)',
    `response_size_bytes_total`             bigint             COMMENT 'Total response size in bytes served to this User-Agent during the hour',
    `cached_response_size_bytes_proportion` double             COMMENT 'Proportion of response bytes served from cache (cache- hit or int)',
    `distinct_ip_count`                     bigint             COMMENT 'Number of distinct IP addresses that sent this User-Agent during the hour',
    `distinct_uri_host_count`               bigint             COMMENT 'Number of distinct URI hosts this User-Agent requested during the hour',
    `hour`                                  timestamp          COMMENT 'The hour during which the User-Agent was seen at the respective source'
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
