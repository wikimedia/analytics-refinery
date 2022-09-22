-- Creates table statement for raw mediawiki_content table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_content_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_content`(
  `content_id`       bigint      COMMENT 'ID of the content object',
  `content_size`     int         COMMENT 'Nominal size of the content object (not necessarily of the serialized blob)',
  `content_sha1`     string      COMMENT 'Nominal hash of the content object (not necessarily of the serialized blob)',
  `content_model`    int         COMMENT 'Reference to content_models.model_id. Note the serialization format isn\'t specified, it should be assumed to be in the default format for the model unless auto-detected otherwise.',
  `content_address`  string      COMMENT 'URL-like address of the content blob. Currently the structure is: tt:<id> where <id> is a number referencing the text.old_id column.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Content_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/content'
;
