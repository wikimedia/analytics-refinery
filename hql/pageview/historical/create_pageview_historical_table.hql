-- Creates table statement for hourly aggregated pageviews table, loaded from
-- pagecounts_ez dumps.
--
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_pageview_historical.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `pageview_historical`(
  `project`        STRING  COMMENT 'Wiki project name, in webhost form, e.g. en.wikipedia',
  `page_title`     STRING  COMMENT 'Page Title from requests path and query',
  `agent_type`     STRING  COMMENT 'Agent accessing the pages. Always "user" in this table, added here to match pageview_hourly\'s schema',
  `access_method`  STRING  COMMENT 'Method used to access the pages. In this table it\'s always "desktop", added here to match pageview_hourly\'s schema',
  `page_id`        BIGINT  COMMENT 'MediaWiki page_id for this page title. In this table it\'s always NULL, added here to match pageview_hourly\'s schema',
  `view_count`     INT     COMMENT 'Number of views for this hour')
PARTITIONED BY (
  `year` int,
  `month` int,
  `day` int,
  `hour` int)
LOCATION '/wmf/data/wmf/pageview/historical'
STORED AS PARQUET
;