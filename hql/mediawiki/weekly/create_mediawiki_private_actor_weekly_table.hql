-- Creates table statement for raw mediawiki_private_actor_weekly table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_mediawiki_private_actor_weekly_table.hql \
--          --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_private_actor_weekly`(
  `actor_id`        bigint      COMMENT 'actor_id is the primary key, used to uniquely identify an actor.',
  `actor_user`      bigint      COMMENT 'actor_user is a key to user.user_id of the user who made the action, or NULL for logged-out (IP) edits and for some mass imports.',
  `actor_name`      string      COMMENT 'actor_name is the text of the username or the IP address of the actor.'
)
COMMENT
  'NOTE: This table contains production-data. It should not be used for public consumption without sanitization. Each day partition is a full weekly snapshot of the actor table. See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Actor_table'
PARTITIONED BY (
  `snapshot` string COMMENT 'The day in YYYY-MM-DD format, the full actor snapshot taken on that day',
  `wiki_db` string COMMENT 'The wiki_db project'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/weekly/actor'
;
