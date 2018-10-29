-- Creates table statement for webrequest_subset_tags table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_webrequest_subset_tags_table.hql --database wmf
--



CREATE EXTERNAL TABLE IF NOT EXISTS `webrequest_subset_tags` (
    `webrequest_tags`    array<string>  COMMENT 'Webrequest tags to direct to a subset (every tag of the list must be present)',
    `webrequest_subset`             string  COMMENT 'subset to be used for that webrequest tag',
    `insertion_dt`       timestamp  COMMENT 'Date of insertion into the list'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
STORED AS TEXTFILE
-- Set table location relative to the current refinery folder
LOCATION '/wmf/refinery/current/static_data/webrequest/subset_tags';
