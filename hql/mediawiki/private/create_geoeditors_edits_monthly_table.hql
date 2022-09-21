-- Creates table statement for geoeditors_monthly table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_geoeditors_monthly_table.hql   \
--          --database wmf
--

CREATE EXTERNAL TABLE `geoeditors_edits_monthly` (
  `wiki_db`                     string   COMMENT 'The wiki database the editors worked in',
  `country_code`                string   COMMENT 'The 2-letter ISO country code this group of edits geolocated to, including Unknown (--)',
  `edits_are_anonymous`         boolean  COMMENT 'Whether or not this group of edits were made anonymously',
  `edit_count`                  bigint   COMMENT 'Number of edits',
  `namespace_zero_edit_count`   bigint   COMMENT 'Number of namespace zero edits'
)
COMMENT
  'Purging this data may be necessary but no decision has been made about it yet.'
PARTITIONED BY (
  `month` string COMMENT 'The month in YYYY-MM format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki_private/geoeditors_edits_monthly'
;
