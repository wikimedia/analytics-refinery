-- Create table statement for country_info table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_geoeditors_blacklist_country_table.hql   \
--          --database wmf
--

CREATE EXTERNAL TABLE `geoeditors_blacklist_country` (
  `country`         string   COMMENT 'The country name',
  `country_code`    string   COMMENT 'The 2-letter ISO country code'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
-- Set table location relative to the current refinery folder
LOCATION '/wmf/refinery/current/static_data/mediawiki/geoeditors/blacklist/';
