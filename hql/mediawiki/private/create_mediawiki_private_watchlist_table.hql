-- Creates table statement for raw mediawiki_private_watchlist table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_private_watchlist_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_private_watchlist`(
  `wl_id`                     bigint      COMMENT 'This is the primary key for the table.',
  `wl_user`                   bigint      COMMENT 'This is the key to user.user_id.',
  `wl_namespace`              int         COMMENT 'This is the key to page_namespace.',
  `wl_title`                  string      COMMENT 'This is the key to page_title.',
  `wl_notificationtimestamp`  string      COMMENT 'This is the timestamp of the first change to the page made by another user after the last visit by the watching user, the timestamp is cleared when the user visits the page. When the timestamp is updated, notification email may be sent (read more on wiki page).'
)
COMMENT
  'NOTE: This table contains production-data. It should be used for public consumption without sanitization. See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Watchlist_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/watchlist'
;
