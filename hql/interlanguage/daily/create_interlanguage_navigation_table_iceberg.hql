-- Create table statement for interlanguage_navigation table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_interlanguage_navigation_table_iceberg.hql    \
--     --database wmf_traffic                           \
--     -d location=/wmf/data/wmf/interlanguage/navigation/daily
--

CREATE EXTERNAL TABLE IF NOT EXISTS `interlanguage_navigation` (
    `project_family`    string  COMMENT 'The project family to aggregate on',
    `current_project`   string  COMMENT 'The project (language) of this group of requests',
    `previous_project`  string  COMMENT 'The project (language) found in the referers of this group of requests',
    `navigation_count`  bigint  COMMENT 'The number of times a user navigated from the previous to the current project',
    `day`                 date  COMMENT 'The day of the aggregation'
)
USING ICEBERG
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'zstd'
)
PARTITIONED BY (years(day))
LOCATION '${location}'
;
