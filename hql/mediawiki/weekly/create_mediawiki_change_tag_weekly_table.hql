-- Creates table statement for raw mediawiki_change_tag_weekly table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_mediawiki_change_tag_weekly_table.hql \
--          --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_change_tag_weekly`(
  `ct_id`                         bigint  COMMENT 'The primary key.',
  `ct_log_id`                     bigint  COMMENT 'log_id for the change.',
  `ct_rev_id`                     bigint  COMMENT 'rev_id for the change. Note that this will not be unique: for each tag applied to a change, a distinct row is added.',
  `ct_tag_id`                     bigint  COMMENT 'Foreign key to primary key of Manual:Change tag def table (change_tag_def.ctd_id).',
  `ct_params`                     string  COMMENT 'Parameters for the tag. Example: gerrit:139881 in Extension:ContentTranslation.'
)
COMMENT
  'Each day partition is a full weekly snapshot of the change_tag table. See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Change_tag_table'
PARTITIONED BY (
  `snapshot` string COMMENT 'The day in YYYY-MM-DD format (the week Wednesday), the full change_tag snapshot taken on that day',
  `wiki_db` string COMMENT 'The wiki_db project')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/weekly/change_tag'
;
