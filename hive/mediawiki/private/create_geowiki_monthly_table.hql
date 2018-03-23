-- Creates table statement for geowiki_monthly table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_geowiki_monthly_table.hql   \
--          --database wmf
--

CREATE EXTERNAL TABLE `geowiki_monthly` (
  `wiki_db`             string      COMMENT 'The wiki database the editors worked in',
  `country_code`        string      COMMENT 'The 2-letter ISO country code this group of editors geolocated to, including Unknown (--)',
  `users_are_anonymous` boolean     COMMENT 'Whether or not this group of editors edited anonymously',
  `activity_level`      string      COMMENT 'How many edits this group of editors performed, can be "at least 1", "at least 5", or "at least 100"',
  `distinct_editors`    bigint      COMMENT 'Number of editors meeting this activity level'
)
COMMENT
  'This table corresponds to the erosen_ tables in analytics-slave.eqiad.wmnet, db "staging".  When a decision is made on how long we should hold on to data there, we can apply the same policy here.'
PARTITIONED BY (
  `month` string COMMENT 'The month in YYYY-MM format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki_private/geowiki_monthly'
;
