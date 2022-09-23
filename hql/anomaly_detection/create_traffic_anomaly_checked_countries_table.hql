-- Create table statement for traffic_anomaly_checked_countries table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_traffic_anomaly_checked_countries_table.hql   \
--          --database wmf
--

CREATE EXTERNAL TABLE `traffic_anomaly_checked_countries` (
  `country`         string   COMMENT 'The country name',
  `country_code`    string   COMMENT 'The 2-letter ISO country code'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
-- Set table location relative to the current refinery folder
LOCATION '/wmf/refinery/current/static_data/data_quality_stats/traffic_anomaly_checked_countries';
