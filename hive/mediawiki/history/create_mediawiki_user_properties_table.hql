-- Creates table statement for raw mediawiki_user_properties table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_user_properties_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_user_properties`(
  `up_user`             bigint      COMMENT 'Foreign key to the user_id field of the user table.',
  `up_property`         string      COMMENT 'The user preference, see Manual:$wgDefaultUserOptions. Examples: gender, rememberpassword, watchcreations',
  `up_value`            string      COMMENT 'The preference value.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:User_properties_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/user_properties'
;
