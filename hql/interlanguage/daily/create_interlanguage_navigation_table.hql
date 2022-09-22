-- Create table statement for interlanguage_navigation table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_interlanguage_navigation_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `interlanguage_navigation` (
    `project_family`    string  COMMENT 'The project family to aggregate on',
    `current_project`   string  COMMENT 'The project (language) of this group of requests',
    `previous_project`  string  COMMENT 'The project (language) found in the referers of this group of requests',
    `navigation_count`  bigint  COMMENT 'The number of times a user navigated from the previous to the current project'
)
PARTITIONED BY (
    `date`              string  COMMENT 'Date in YYYY-MM-DD format'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/interlanguage/navigation/daily'
;
