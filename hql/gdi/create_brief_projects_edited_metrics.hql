CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`brief_projects_edited_metrics` (
  `country_code`                        string   COMMENT 'ISO2 country code',
  `country_name`                        string   COMMENT 'Country name',
  `average_monthly_acitve_editors`      double   COMMENT 'Monthly Active Editor average',
  `proportion`                          double   COMMENT 'Proportion editors',
  `wiki_db`                             string   COMMENT 'Wiki database name',
  `project_label`                       string   COMMENT 'List label',
  `language`                            string   COMMENT 'Language name'
  )
COMMENT  'Contains brief projects edited metrics according to language'
PARTITIONED BY (
  `year` int COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/brief_projects_edited_metrics'
;