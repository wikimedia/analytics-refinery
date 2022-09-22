-- Creates table statement for raw wikibase_wbt_text_in_lang table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_wikibase_wbt_text_in_lang_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `wikibase_wbt_text_in_lang`(
  `wbxl_id`                         bigint  COMMENT 'The primary key.',
  `wbxl_language`                   string  COMMENT 'Language of the string. Example, en',
  `wbxl_text_id`                    bigint  COMMENT 'Relation to the wbt_text table'
)
COMMENT
  'Modern secondary wikibase term storage.'
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
  'hdfs://analytics-hadoop/wmf/data/raw/wikibase/tables/wbt_text_in_lang'
;