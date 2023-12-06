-- Creates table statement for raw mediawiki_private_linktarget table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_private_linktarget_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_private_linktarget`(
  `lt_id`           bigint  COMMENT 'Primary key.',
  `lt_namespace`    int     COMMENT 'Namespace of the link target.',
  `lt_title`        string  COMMENT 'Text part of link target excluding namespace.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Linktarget_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/linktarget'
;
