-- Creates table statement for mobile apps uniques monthly table.
-- This table uses the archive directory as a base and gets updated
-- through file move (mobile_apps/uniques/monthly oozie job).
-- It uses TSV format to store / retrieve data, facilitatig
-- file reusability.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mobile_apps_uniques_monthly_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `mobile_apps_uniques_monthly`(
    `year`                 int     COMMENT 'Unpadded year of request',
    `month`                int     COMMENT 'Unpadded month of request',
    `platform`             string  COMMENT 'Mobile platform from user agent parsing',
    `unique_count`         bigint  COMMENT 'Distinct uuid count'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '/wmf/data/archive/mobile_apps/uniques/monthly'
;
