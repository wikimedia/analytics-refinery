-- Creates table statement for mediawiki_metrics table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_metrics_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE `mediawiki_metrics`(
  `dt`      string  COMMENT 'The date of this measurement, as YYYY-MM-DD',
  `metric`    string  COMMENT 'The metric being computed to measure',
  `wiki_db`   string  COMMENT 'The wiki this measurement pertains to',
  `value`   bigint  COMMENT 'The measurement'
)
COMMENT
  'See most up to date documentation at https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Metric_results'
PARTITIONED BY (
  `snapshot`  string  COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular labs imports)'
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki/metrics'
;
