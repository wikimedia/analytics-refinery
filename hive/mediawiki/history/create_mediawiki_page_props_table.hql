-- Creates table statement for raw mediawiki_page_props table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_page_props_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_page_props`(
  `pp_page`             bigint      COMMENT 'page_id by which the name/value pair is indexed',
  `pp_propname`         string      COMMENT 'Page property name',
  `pp_value`            string      COMMENT 'Page property value',
  `pp_sortkey`          float       COMMENT 'This is so pages can be efficiently queried and sorted by property value (see task T60032).'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Page_props_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/page_props'
;
