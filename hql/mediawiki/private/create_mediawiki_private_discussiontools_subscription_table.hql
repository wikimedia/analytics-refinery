-- Creates table statement for raw mediawiki_private_comment table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_private_comment_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_private_discussiontools_subscription`(
  `sub_id`          bigint  COMMENT '',
  `sub_item`        string  COMMENT '',
  `sub_namespace`   int     COMMENT '',
  `sub_title`       string  COMMENT '',
  `sub_section`     string  COMMENT '',
  `sub_state`       int     COMMENT '',
  `sub_user`        bigint  COMMENT '',
  `sub_created`     string  COMMENT '',
  `sub_notified`    string  COMMENT ''
)
COMMENT
  ''
PARTITIONED BY (
  `snapshot` string COMMENT 'In this case, the snapshot will always be set to latest to allow a whole-table import to overwrite the previous data.  This is an experimental approach predicated on small data.  Increased data sizes and retention policies may imply changes in the future.  NOTE: This means that we do not need the usual mediawiki/history/load job as all other similarly sqooped tables need.  Since we always overwrite, there is no need to msck repair table, the partitions stay the same, only the data changes.  It remains to be seen whether this causes any weird behavior.',
  `wiki_db` string COMMENT 'The wiki_db project')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/discussiontools_subscription'
;
