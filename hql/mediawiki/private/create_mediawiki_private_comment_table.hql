-- Creates table statement for raw mediawiki_private_comment table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_private_comment_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_private_comment`(
  `comment_id`        bigint      COMMENT 'Unique ID to identify each comment.',
  `comment_text`      string      COMMENT 'Text comment summarizing the change, e.g. an editor\'s edit summary. This text is shown in the history and contributions. It is also used for recent changes, related changes, watchlists, and, in the case of page creation, for the list of new pages. It is rendered in a sanitized subset of wiki markup by Linker::formatComment(). Size limits are enforced at the application level, and should take care to crop UTF-8 strings appropriately.'
)
COMMENT
  'NOTE: This table contains production-data. It should be used for public consumption without sanitization. See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Comment_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/comment'
;
