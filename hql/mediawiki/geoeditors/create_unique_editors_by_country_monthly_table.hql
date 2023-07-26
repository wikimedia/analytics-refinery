-- Creates table statement for unique_editors_by_country_monthly table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_editors_by_country_monthly_table.hql.hql   \
--          --database wmf
--

CREATE EXTERNAL TABLE `unique_editors_by_country_monthly` (
  `country_code`                    string      COMMENT 'The 2-letter ISO country code this group of editors geolocated to, including Unknown (--)',
  `users_are_anonymous`             boolean     COMMENT 'Whether or not this group of editors edited anonymously',
  `activity_level`                  string      COMMENT 'How many edits this group of editors performed, can be "1 to 4", "5 to 99", or "100 or more"',
  `distinct_editors`                bigint      COMMENT 'Number of editors meeting this activity level',
  `namespace_zero_distinct_editors` bigint      COMMENT 'Number of editors meeting this activity level with only namespace zero edits'
)
COMMENT
  'This table contains unique editor counts by country, regardless of wiki project.'
PARTITIONED BY (
  `month` string COMMENT 'The month in YYYY-MM format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki_private/unique_editors_by_country_monthly'
;
