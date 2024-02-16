-- Create table statement for cx_abuse_filter_daily table.
--
-- Parameters:
--     destination_table    -- Name of the table to create
--                             (database name provided through spark3-sql
--                              command-line argument).
--     location             -- HDFS folder path to place the table files in.
--
-- Usage
--     spark3-sql -f create_cx_daily_table.hql             \
--     --database wmf_product                              \
--     -d destination_table=cx_abuse_filter_daily          \
--     -d location=/wmf/data/wmf_product/cx_abuse_filter


CREATE EXTERNAL TABLE IF NOT EXISTS `${destination_table}`(
    `project`         string COMMENT 'wiki db name',
    `abuse_filter_id` bigint COMMENT 'AbuseFilter ID in that wiki',
    `trigger_count`   bigint COMMENT 'Value of the metric.',
    `day`             date   COMMENT 'The day for which the metric is computed over.'
)
USING ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION '${location}'
;


