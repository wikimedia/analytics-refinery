CREATE EXTERNAL TABLE IF NOT EXISTS gdi.wiki_db_map_input_metrics (
  database_code     string  comment 'database code',
  database_group    string  comment 'database group',
  grouped_bin       string  comment 'grouped bin name',
  language_code     string  comment 'language code',
  language_name     string  comment 'language name'
  )
COMMENT  'Table to store the wiki project mapping to project database'
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/wiki_db_map_input_metrics';