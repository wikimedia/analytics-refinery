-- Creates table statement for raw mediawiki_slots table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_slots_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_slots`(
  `slot_revision_id`    bigint      COMMENT 'Reference to revision.rev_id or archive.ar_rev_id. slot_revision_id and slot_role_id together comprise the primary key with wiki_db.',
  `slot_role_id`        int         COMMENT 'Reference to slot_roles.role_id',
  `slot_content_id`     bigint      COMMENT 'Reference to content.content_id',
  `slot_origin`         bigint      COMMENT 'The revision.rev_id of the revision that originated the slot\'s content. To find revisions that changed slots, look for slot_origin = slot_revision_id.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Slots_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/slots'
;
