-- Creates table statement for raw mediawiki_imagelinks table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_imagelinks_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_imagelinks`(
  `il_from`             bigint      COMMENT 'The page_id of the page the link is on',
  `il_to`               string      COMMENT 'The title of the file being linked to (Its value is null because it is deprecated since Feb, 2026. See https://phabricator.wikimedia.org/T416481)',
  `il_from_namespace`   int         COMMENT 'page_namespace of the page containing the link',
  `il_target_id`        bigint      COMMENT 'Foreign key to linktarget.lt_id'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Imagelinks_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/imagelinks'
;
