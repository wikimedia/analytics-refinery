-- Creates table statement for raw mediawiki_change_tag_def table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_change_tag_def_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_change_tag_def`(
  `ctd_id`                     bigint  COMMENT 'Numerical ID of the tag (change_tag.ct_tag_id refers to this)',
  `ctd_name`                   string  COMMENT 'Symbolic name of the tag (what would previously be put in change_tag.ct_tag)',
  `ctd_user_defined`           boolean COMMENT 'Whether this tag was defined manually by a privileged user using Special:Tags',
  `ctd_count`                  bigint  COMMENT 'Number of times this tag was used'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Change_tag_def_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/change_tag_def'
;