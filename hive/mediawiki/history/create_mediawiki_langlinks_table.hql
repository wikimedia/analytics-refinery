-- Creates table statement for raw mediawiki_langlinks table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_langlinks_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_langlinks`(
  `ll_from`          bigint  COMMENT 'page_id of the referring page.',
  `ll_lang`          string  COMMENT 'Language code of the target, in the ISO 639-1 standard.',
  `ll_title`         string  COMMENT 'Title of the target, including namespace (FULLPAGENAMEE style).'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Langlinks_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/langlinks'
;
