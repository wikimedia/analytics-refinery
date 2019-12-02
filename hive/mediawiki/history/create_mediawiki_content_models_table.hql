-- Creates table statement for raw mediawiki_content_models table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_content_models_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_content_models`(
  `model_id`    int         COMMENT 'Primary key with wiki_db.',
  `model_name`  string      COMMENT 'Description of the name of the model.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Content_models_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/content_models'
;
