-- Creates an Iceberg table to store daily or monthly aggregated pageview counts of users
--
-- Note: This table only includes data about real user account editors.
--   No IP or Temp editors here.
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
--     spark3-sql -f create_pageviews_per_editor.hql \
--                -d destination_table=wmf_readership.pageviews_per_editor \
--                -d location=hdfs://analytics-hadoop/wmf/data/wmf_readership/pageviews_per_editor
--
CREATE TABLE IF NOT EXISTS ${destination_table} (
    `user_central_id` bigint  COMMENT 'MediaWiki user central id. At Wikimedia, this is the CentralAuth globaluser gu_id field',
    `granularity`     string  COMMENT 'Daily or monthly',
    `view_count`      bigint  COMMENT 'Count of pageviews on all pages edited by this user for the given time period (daily or monthly)',
    `dt`              timestamp  COMMENT 'The date for which we aggregate the data. It points to the beginning of the granularity period (e.g. for daily: YYYY-MM-DDT00:00:00.000Z, for monthly: YYYY-MM-01T00:00:00.000Z)'
)
USING ICEBERG
PARTITIONED BY (MONTHS(dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd'
)
COMMENT 'Stores pageview count gotten on all pages ever edited by a user for a date period (it could be daily or monthly).'
LOCATION '${location}'
;
