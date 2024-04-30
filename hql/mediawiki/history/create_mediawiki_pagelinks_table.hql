-- Creates table statement for raw mediawiki_pagelinks table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_pagelinks_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_pagelinks`(
  `pl_from`             bigint      COMMENT 'Key to the page_id of the page containing the link',
  `pl_from_namespace`   int         COMMENT 'MediaWiki version:  ≥ 1.24 - page_namespace of the page containing the link',
  `pl_target_id`        bigint      COMMENT 'Foreign key to linktarget.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Pagelinks_table'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular labs imports)',
  `wiki_db` string COMMENT 'The wiki_db project')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/pagelinks'
;
