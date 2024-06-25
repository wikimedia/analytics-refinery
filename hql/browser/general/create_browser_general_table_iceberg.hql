-- Create table statement for browser general data.
--
-- This is an intermediate table that serves as a base for various
-- traffic reports, i.e.: mobile web browser breakdown, desktop os
-- breakdown, or desktop+mobile web os+browser breakdown, etc.
-- It is partitioned by year, month and day, and it's an external
-- table stored as parquet.
--
-- Note that the long tail of the table (meaning the rows that have
-- a view_count relatively smaller than a given threshold) are collapsed
-- into a single row with all dimension values equal to 'Unknown'
-- (some columns may have a default value different from 'Unknown').
-- This ensures the data in this table is not privacy sensitive,
-- and that the size of the files keeps considerably small.
--
-- Usage
--     spark3-sql -f create_browser_general_table_iceberg.hql \
--          --database wmf_traffic \
--          -d location=/wmf/data/wmf_traffic/browser/general
--

CREATE EXTERNAL TABLE IF NOT EXISTS `browser_general`(
    `access_method`     string     COMMENT '(desktop|mobile web|mobile app)',
    `os_family`         string     COMMENT 'OS family: Windows, Android, etc.',
    `os_major`          string     COMMENT 'OS major version: 8, 10, etc.',
    `browser_family`    string     COMMENT 'Browser family: Chrome, Safari, etc.',
    `browser_major`     string     COMMENT 'Browser major version: 47, 11, etc.',
    `view_count`        bigint     COMMENT 'Number of pageviews.',
    `day`               date       COMMENT 'The date of request.'
)
USING ICEBERG
LOCATION '${location}'
;
