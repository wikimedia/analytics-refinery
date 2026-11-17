-- Creates an Iceberg table to store daily or monthly aggregated pageview counts of users per wiki
--
-- Note: This table only includes data about real user account editors.
--   No IP or Temp editors here.The filtering has been done in the
--   upstream table wmf_contributors.edit_per_editor_per_page_daily.
--
-- Parameters:
--     destination_table
--         Database and name under which the created
--         table will be registered
--
--     location
--         Directory location in which to store the data.
--
-- Usage:
--     spark3-sql -f create_pageviews_per_editor_per_wiki.hql \
--                -d destination_table=wmf_readership.pageviews_per_editor_per_wiki \
--                -d location=hdfs://analytics-hadoop/wmf/data/wmf_readership/pageviews_per_editor_per_wiki
--
CREATE TABLE IF NOT EXISTS ${destination_table} (
    `user_central_id` bigint  COMMENT 'MediaWiki user central id. At Wikimedia, this is the CentralAuth globaluser gu_id field',
    `wiki_id`         string  COMMENT 'The wiki ID',
    `granularity`     string  COMMENT 'The length of the aggregation period: daily or monthly.',
    `view_count`      bigint  COMMENT 'Count of pageviews on all pages edited by this user on this wiki during the granularity period',
    `dt`              timestamp  COMMENT 'The start of the granularity period (inclusive), in UTC. Example daily: if dt = 2025-10-25T00:00:00.000Z it covers 2025-10-25T00:00:00.000Z (inclusive) to 2025-10-26T00:00:00.000Z (exclusive). For monthly: if dt = 2025-10-01T00:00:00.000Z, it covers 2025-10-01T00:00:00.000Z (inclusive) to 2025-11-01T00:00:00.000Z (exclusive).'
)
USING ICEBERG
PARTITIONED BY (MONTHS(dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd'
)
COMMENT 'Stores pageview count gotten on all pages ever edited by a user on a given wiki for a date period (it could be daily or monthly).'
LOCATION '${location}'
;
