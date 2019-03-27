-- Create table statement for country_info table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_country_info_table.hql   \
--          --database wmf_raw
--

CREATE EXTERNAL TABLE `country_info` (
  `country`         string   COMMENT 'The country name as found in MaxMind databases',
  `country_code`    string   COMMENT 'The 2-letter ISO country code'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
-- Set table location relative to the current refinery folder
LOCATION '/wmf/refinery/current/static_data/general/country_info';
