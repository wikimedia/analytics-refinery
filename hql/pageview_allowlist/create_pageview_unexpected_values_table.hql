-- Create table statement for pageview unexpected values table.
--
-- Parameters:
--     --database
--
-- Usage
--     hive -f create_pageview_unexpected_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `pageview_unexpected_values`(
    `field_name`        string  COMMENT 'Name of the field with unexpected value',
    `unexpected_value`  string  COMMENT 'Value not in the allow-list for the given field name',
    `view_count`        bigint  COMMENT 'Number of views of the unexpected value'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of unexpected requests',
    `month`             int     COMMENT 'Unpadded month of unexpected requests',
    `day`               int     COMMENT 'Unpadded day of unexpected requests'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/wmf/data/wmf/pageview/unexpected_values'
;
