CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`geoeditor_input_metrics` (
  `country_code`                string      COMMENT 'The 2-letter ISO country code this group of actions geolocated to, including Unknown (--)',
  `distinct_editors`            double      COMMENT 'The number of distinct editors who performed the actions in this group (sum)',
  `grouped_bin`                 string      COMMENT 'The bin the wiki_db is in e.g. wikidata',
  `month`                       string      COMMENT 'The month the actions were performed in')
COMMENT
  'This table contains metrics data from geoeditors_monthly grouped by bins from wiki_db_map_input_metrics'
PARTITIONED BY (
  `year` int COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/geoeditor_input_metrics'
;
