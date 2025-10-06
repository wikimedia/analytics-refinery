-- Creates table statement for raw centralauth_localuser table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql \
--       --database wmf_raw \
--       -f create_centralauth_localuser_table.hql \
--

CREATE EXTERNAL TABLE `centralauth_localuser`(
  lu_wiki string,
  lu_name string,
  lu_attached_timestamp string COMMENT 'Migration status/logging information, to help diagnose issues',
  lu_attached_method string COMMENT 'Migration status/logging information, to help diagnose issues. See CentralAuthUser::attach().',
  lu_attachment_method boolean COMMENT 'Part of a pending schema migration for lu_attached_method (T304015).',
  lu_local_id bigint,
  lu_global_id bigint
)
COMMENT
  'Local linkage info, listing which wikis the username is attached to the global account. All local DBs will be swept on an opt-in check event. See most up to date documentation at https://www.mediawiki.org/wiki/Extension:CentralAuth/globaluser_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/centralauth_localuser'
;
