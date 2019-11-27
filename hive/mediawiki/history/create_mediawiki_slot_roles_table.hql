-- Creates table statement for raw mediawiki_slot_roles table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_slot_roles_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_slot_roles`(
  `role_id`    int         COMMENT 'Primary key with wiki_db.',
  `role_name`  string      COMMENT 'Description of the name of the role.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Slot_roles_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/slot_roles'
;
