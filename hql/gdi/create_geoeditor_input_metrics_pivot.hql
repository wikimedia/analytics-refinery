CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`geoeditor_input_metrics_pivot` (
  `country_code`                string     COMMENT 'The 2-letter ISO country code this group of actions geolocated to, including Unknown (--)',
  `commons`                     double     COMMENT 'Represents the wiki commons namespace',
  `mediawiki`                   double     COMMENT 'Represents the MediaWiki namespace',
  `wikidata`                    double     COMMENT 'Represents the Wikidata namespace',
  `wikipedia`                   double     COMMENT 'Represents the Wikipedia namespace',
  `wikisource`                  double     COMMENT 'Represents the Wikisource namespace',
  `sister_project`              double     COMMENT 'Represents the sister project namespace',
  `organizing_wiki`             double     COMMENT 'Represents the wiki that is the primary source of information for the project'
  )
COMMENT
  'This table is a pivot of the data from geoeditor_input_metrics table.'
PARTITIONED BY (
  `year`   int COMMENT 'The year in YYYY format',
  `metric` string COMMENT 'The metric being measured, e.g. monthly_bins, ann_presence, yoy_change,annual_growth, presence_by_growth, editorship_percentiles, etc'
  )
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/geoeditor_input_metrics_pivot'
;
