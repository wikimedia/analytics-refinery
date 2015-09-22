-- Creates table statement for pageview unexpected values table.
--
-- NOTE:  When choosing partition field types,
-- one should take into consideration Hive's
-- insistence on storing partition values
-- as strings.  See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png
-- and
-- http://bots.wmflabs.org/~wm-bot/logs/%23wikimedia-analytics/20140721.txt
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_pageview_unexpected_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `pageview_unexpected_values`(
    `field_name`        string  COMMENT 'Name of the field with unexpected value',
    `unexpected_value`  string  COMMENT 'Value not in the whitel-ist for the given field name',
    `view_count`        bigint  COMMENT 'Number of views of the unexpected value'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of request',
    `month`             int     COMMENT 'Unpadded month of request',
    `day`               int     COMMENT 'Unpadded day of request',
    `hour`              int     COMMENT 'Unpadded hour of request'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/wmf/data/wmf/pageview/unexpected_values'
;