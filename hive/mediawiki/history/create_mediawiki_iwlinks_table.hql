-- Creates table statement for raw mediawiki_iwlinks table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_iwlinks_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_iwlinks`(
  `iwl_from`            bigint  COMMENT 'The page_id of the referring page (i.e. the local page that the link appears on).',
  `iwl_prefix`          string  COMMENT 'interwiki prefix code of the target. This is used the same way as a namespace is used when editing.',
  `iwl_title`           string  COMMENT 'The title of the target page, including namespace (FULLPAGENAME style). Spaces are converted to underscores, but the first letter is not automatically capitalized. So for example, an interwiki link to w:foo bar would have an iwl_title of "foo_bar".'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Iwlinks_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/iwlinks'
;
