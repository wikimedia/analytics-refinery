-- Creates a table backed by a file in static_data
--   This allows grouping namespaces for analysis
--   Initially created to help with coarse reader activity metrics
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_namespace_groups_table.hql --database wmf_traffic
--



CREATE EXTERNAL TABLE IF NOT EXISTS `namespace_groups` (
    `group_name`    string  COMMENT 'Coarse group for this namespace id and pages in this namespace',
    `namespace_id`  int  COMMENT 'Foreign key to MariaDB -> page.page_namespace'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
-- always get the latest version of the static files (current)
LOCATION '/wmf/refinery/current/static_data/namespace/groups';
