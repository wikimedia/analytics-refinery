-- Creates table statement for raw mediawiki_change_tag table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_change_tag_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_change_tag`(
  `ct_rev_id`                     bigint  COMMENT 'Revision ID from the revision table',
  `ct_tag_id`                     bigint  COMMENT 'Primary key, corresponding with ct_id in change_tag_ref',
  `ctd_name`                      string  COMMENT 'Symbolic name of the tag'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Change_tag_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/change_tag'
;