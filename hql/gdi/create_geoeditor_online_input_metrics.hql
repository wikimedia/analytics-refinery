CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`geoeditor_online_input_metrics` (
  `country_code`                    string      COMMENT 'The 2-letter ISO country code this group of actions geolocated to, including Unknown (--)',
  `monthly_distinct_editors`        double      COMMENT 'The number of distinct editors who have edited the page in the month',
  `monthly_edits_avg`               double      COMMENT 'The average number of edits per editor in the month',
  `percent_editors_active`          double      COMMENT 'monthly_distinct_editors / monthly_edits_avg'
  )
COMMENT
  'This table contains data related to the number of editors who performed actions in a given country and their percentiles'
PARTITIONED BY (
  `year` int COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/geoeditor_online_input_metrics'
;
