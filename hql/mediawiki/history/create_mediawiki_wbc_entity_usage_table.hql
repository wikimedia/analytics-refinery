-- Creates table statement for raw mediawiki_wbc_entity_usage table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_wbc_entity_usage_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_wbc_entity_usage`(
  `eu_row_id`      bigint      COMMENT 'auto-increment row ID for internal use. Primary key with wiki_db.',
  `eu_entity_id`   string      COMMENT 'ID of the entity used.',
  `eu_aspect`      string      COMMENT 'Aspect of the entity used.',
  `eu_page_id`     bigint      COMMENT 'the ID of the page using the entity, refers to page.page_id.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Wikibase/Schema/wbc_entity_usage'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/wbc_entity_usage'
;
