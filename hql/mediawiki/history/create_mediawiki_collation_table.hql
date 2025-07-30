-- Creates table statement for raw mediawiki_collation table.
-- Normalization table for collation names.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_collation_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_collation`(
  `collation_id`    int     COMMENT 'Primary key.',
  `collation_name`  string  COMMENT 'Name of the collation.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Collation_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/collation'
;
