-- Creates table statement for raw wikibase_wb_terms table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_wikibase_wb_terms_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `wikibase_wb_terms`(
  `term_row_id`                         bigint  COMMENT 'The primary key.',
  `term_entity_id`                      bigint  COMMENT 'entity id as an integer. Example Q123 -> 123',
  `term_entity_type`                    string  COMMENT 'entity type as a string. Example item or property',
  `term_language`                       string  COMMENT 'term language as a string. Example en',
  `term_type`                           string  COMMENT 'term type as a string. Example label',
  `term_text`                           string  COMMENT 'term text as a string. Example Berlin'
)
COMMENT
  'Legacy secondary wikibase term storage, will be removed in 2020'
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
  'hdfs://analytics-hadoop/wmf/data/raw/wikibase/tables/wb_terms'
;