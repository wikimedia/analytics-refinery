-- Creates table statement for geowiki_daily table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_geowiki_daily_table.hql   \
--          --database wmf
--

CREATE EXTERNAL TABLE `geowiki_daily` (
  `wiki_db`             string      COMMENT 'The wiki database of origin',
  `country_code`        string      COMMENT 'The 2-letter ISO country code this group of edits geolocated to, including Unknown (--)',
  `user_id_or_ip`       string      COMMENT 'If an anonymous user, this is the ip of the user, otherwise it is their user id in this wiki db',
  `user_is_anonymous`   boolean     COMMENT 'Whether or not this user edited this group of edits anonymously',
  `date`                string      COMMENT 'The YYYY-MM-DD date for this group of edits',
  `edit_count`          bigint      COMMENT 'The total count of edits for this group of edits'
)
COMMENT
  'NOTE: This table has private location data in it.  It should be truncated so no data older than 90 days is available.  See up to date schema documentation for the source table at https://www.mediawiki.org/wiki/Extension:CheckUser/cu_changes_table'
PARTITIONED BY (
  `month` string COMMENT 'The month in YYYY-MM format, all edits from that month from the cu_changes table are aggregated in this partition'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki_private/geowiki_daily'
;
