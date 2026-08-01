--
-- Creates a table to store Cloudflare Radar's bot directory: a vendor-maintained catalog of
-- https://radar.cloudflare.com/traffic/verified-bots
-- https://developers.cloudflare.com/api/resources/radar/subresources/bots/methods/get
--
-- Parameters:
--     table_name      Fully qualified name of the table to create.
--     base_directory  HDFS path to use as the table's base location.
--
-- Usage:
--     spark3-sql -f create_cloudflare_bot_directory_table.hql \
--         -d table_name=wmf_traffic.cloudflare_bot_directory \
--         -d base_directory=/wmf/data/wmf_traffic/cloudflare_bot_directory
--
-- 
CREATE TABLE IF NOT EXISTS ${table_name} (
    `slug`                    string        COMMENT 'Kebab-case identifier for the bot in the Cloudflare bot directory',
    `bot_name`                string        COMMENT 'Name of the bot',
    `bot_kind`                string        COMMENT 'Kind of the bot, e.g. bot or agent',
    `bot_operator`            string        COMMENT 'Organization that owns and operates the bot',
    `bot_operator_url`        string        COMMENT 'URL of the bot operator organization or the documentation',
    `bot_category`            string        COMMENT 'Category of the bot',
    `bot_description`         string        COMMENT 'Summary of the bot',
    `user_agent_patterns`     array<string> COMMENT 'Identified patterns used to match this bot against User-Agent strings',
    `user_agents`             array<string> COMMENT 'Literal User-Agent strings for this bot; in most cases only contains examples, but not an exhaustive list',
    `snapshot_day`            date          COMMENT 'Day this snapshot of the bot directory was ingested'
)
USING ICEBERG
PARTITIONED BY (years(`snapshot_day`))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION '${base_directory}'
;
