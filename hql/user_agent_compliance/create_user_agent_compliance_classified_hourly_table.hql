--
-- Creates a table to store classified User-Agents per WMF UA compliance policy.
-- Each partition holds the distinct User-Agents seen in that hour and their classification,
-- Downstream jobs can left-join as required.
--
-- Parameters:
--     table_name      Fully qualified name of the table to create.
--     base_directory  HDFS path to use as the table's base location.
--
-- Usage:
--     spark3-sql -f create_user_agent_compliance_classified_hourly_table.hql \
--         -d table_name=wmf_traffic.user_agent_compliance_classified_hourly \
--         -d base_directory=/wmf/data/wmf_traffic/user_agent_compliance_classified_hourly
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
    `user_agent`                string        COMMENT 'User-Agent string',
    `contact_method`            array<string> COMMENT 'Detected contact method types (email, url, mw_user, placeholder_email, placeholder_url)',
    `contact_info`              array<string> COMMENT 'Detected contact info values corresponding to each contact_method entry',
    `compliance_level`          string        COMMENT 'compliant, partially_compliant, non_compliant, non_compliant_placeholder, or browser_like',
    `is_instant_commons_agent`  boolean       COMMENT 'True if the User-Agent contains QuickInstantCommons or InstantCommons',
    `is_foreign_api_agent`      boolean       COMMENT 'True if the User-Agent contains ForeignAPIRepo',
    `is_wikipedia_app_agent`    boolean       COMMENT 'True if the User-Agent contains WikipediaApp',
    `hour`                      timestamp     COMMENT 'The hour during which the User-Agent was seen'
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
