-- Creates table statement for raw mediawiki_templatelinks table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_templatelinks_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_templatelinks`(
  `tl_from`           bigint  COMMENT 'Key to the page_id of the page containing the link.',
  `tl_from_namespace` int     COMMENT 'page_namespace of the page containing the link.',
  `tl_namespace`      int     COMMENT 'Key to page_namespace of the target page. Used in conjunction with tl_title.',
  `tl_title`          string  COMMENT 'Key to page_title of the target page. Used in conjunction with tl_namespace.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Templatelinks_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/templatelinks'
;
