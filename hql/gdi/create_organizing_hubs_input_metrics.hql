CREATE EXTERNAL TABLE IF NOT EXISTS gdi.organizing_hubs_input_metrics (
  `hub_type`                string      COMMENT 'The type of hub',
  `group_type`              string      COMMENT 'The type of group',
  `group_name`              string      COMMENT 'The name of the group',
  `group_code`              string      COMMENT 'The code of the group',
  `previous_year_status`    string      COMMENT 'The status of the group in the previous year',
  `current_year_status`     string      COMMENT 'The status of the group in the current year',
  `alt_page_associated`     string      COMMENT 'The alternative page associated with the group'
)
COMMENT
  'Stores the organizing hubs CSV file'
PARTITIONED BY (
  `year`      int    COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/gdi/organizing_hubs_input_metrics';
