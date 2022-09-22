-- Creates table statement for pageview whitelist table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_pageview_whitelist_table.hql --database wmf
--



CREATE EXTERNAL TABLE IF NOT EXISTS `pageview_whitelist` (
    `field_name`        string  COMMENT 'Name of the field with a white-listed value',
    `authorized_value`  string  COMMENT 'Value authorized for the given field name',
    `insertion_dt`      timestamp  COMMENT 'Date of insertion into the white-list'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
-- Set table location relative to the current refinery folder
LOCATION '/wmf/refinery/current/static_data/pageview/whitelist';
