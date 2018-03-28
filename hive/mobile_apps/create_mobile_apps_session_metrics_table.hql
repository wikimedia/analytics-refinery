-- NOTE: This table only hold historical data, no new data is generated as of
-- 2018-03
--
-- Creates table statement for mobile apps session metrics uniques daily table.
-- This table uses the archive directory as a base and gets updated
-- through a Spark job run every 7 days (mobile_apps/session_metrics oozie job).
-- It uses TSV format to store / retrieve data, facilitating
-- file reusability.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mobile_apps_session_metrics_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `mobile_apps_session_metrics`(
    `year`                 int     COMMENT 'Unpadded year of report run date',
    `month`                int     COMMENT 'Unpadded month of report run date',
    `day`                  int     COMMENT 'Unpadded day of report run date',
    `date_range`           string  COMMENT 'Period for which report was run',
    `type`                 string  COMMENT 'Type of session metric',
    `count`                int     COMMENT 'Value of count for given metric',
    `min`                  int     COMMENT 'Min value for given metric',
    `max`                  int     COMMENT 'Max value for given metric',
    `p_1`                  string  COMMENT '1st Percentile for given metric',
    `p_50`                 string  COMMENT '50th Percentile for given metric',
    `p_90`                 string  COMMENT '90th Percentile for given metric',
    `p_99`                 string  COMMENT '99th Percentile for given metric'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '/wmf/data/wmf/mobile_apps/session_metrics'
;