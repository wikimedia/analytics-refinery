-- Creates table statement for raw mediawiki_pagelinks table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_pagelinks_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_pagelinks`(
  `pl_from`             bigint      COMMENT 'Key to the page_id of the page containing the link',
  `pl_namespace`        int         COMMENT 'Key to page_namespace of the target page. The target page may or may not exist, and due to renames and deletions may refer to different page records as time goes by',
  `pl_title`            string      COMMENT 'Key to page_title of the target page. The target page may or may not exist, and due to renames and deletions may refer to different page records as time goes by. Spaces are converted to underscores, and the first letter is automatically capitalized. So for example, a link to foo bar would have a pl_title of "Foo_bar"',
  `pl_from_namespace`   int         COMMENT 'MediaWiki version:  ≥ 1.24 - page_namespace of the page containing the link'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Pagelinks_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/pagelinks'
;
