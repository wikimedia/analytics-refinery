-- Creates table statement for raw mediawiki_ipblocks_restrictions table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_ipblocks_restrictions_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_ipblocks_restrictions`(
  `ir_ipb_id`               bigint  COMMENT 'Foreign key to ipb_id in ipblocks table',
  `ir_type`                 bigint  COMMENT 'The type of the block (1: Page, 2: Namespace)',
  `ir_value`                bigint  COMMENT 'The value of the block, eg. page_id, or namespace id from the config'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Ipblocks_restrictions_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/ipblocks_restrictions'
;
