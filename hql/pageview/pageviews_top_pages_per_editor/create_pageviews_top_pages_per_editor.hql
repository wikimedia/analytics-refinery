-- Creates an Iceberg table to store top k viewed pages per editor for a month
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
--     spark3-sql -f create_pageviews_top_pages_per_editor.hql \
--                -d destination_table=wmf_readership.pageviews_top_pages_per_editor \
--                -d location=hdfs://analytics-hadoop/wmf/data/wmf_readership/pageviews_top_pages_per_editor
--

CREATE TABLE IF NOT EXISTS ${destination_table} (
    `user_central_id` bigint  COMMENT 'MediaWiki user central id. At Wikimedia, this is the CentralAuth globaluser gu_id field',
    `granularity`     string  COMMENT 'monthly',
    `wiki_id`         string     COMMENT 'Wiki id, sometimes also called just wiki or database name',
    `page_id`         bigint  COMMENT 'MediaWiki page id',
    `rank`            int     COMMENT 'Rank of this page among all pages edited by this user, based on monthly pageviews. The most viewed page has rank 1',
    `top_k`	          int     COMMENT 'Limit of top viewed pages used when calculating this month\'s data. When k = 10, there will be max 10 page records per user per month',
    `view_count`      bigint  COMMENT 'Monthly pageview count for this page (aggregated across all days in the month)',
    `dt`              timestamp  COMMENT 'The date for which we aggregate the data. It should point to the beginning of the granularity period (e.g. for monthly: YYYY-MM-01T00:00:00.000Z)'
)
USING ICEBERG
PARTITIONED BY (MONTHS(dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd'
)
COMMENT 'Stores k pages with top pageviews per editor for a month (k is an integer e.g 10)'
LOCATION '${location}'
;
