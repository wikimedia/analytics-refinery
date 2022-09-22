-- Creates table statement for mediawiki_history_reduced table.
--
-- WARNING: Timestamp fields are commented in that files
-- because our version of hive doesn't support them.
-- Waiting for us to upgrade to hive 1.2 or higher
-- to update the fields.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_history_reduced_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `mediawiki_history_reduced` (
  `project`              string COMMENT 'The project this event belongs to (en.wikipedia or wikidata for instance)',
  `event_entity`         string COMMENT 'revision, user, page',
  `event_type`           string COMMENT 'create, move, delete, etc with specific digest types.  Detailed explanation in the docs under #Event_types',
  `event_timestamp`      string COMMENT 'When this event ocurred',
  `user_text`            string COMMENT 'user_text of user performing the event, whether registered or anonymous (IP)',
  `user_type`            string COMMENT 'anonymous, group_bot, name_bot or user',
  `page_title`           string COMMENT 'The page_title of the event, prefixed with canonical namespace if any',
  `page_namespace`       int    COMMENT 'The page namespace of the event',
  `page_type`            string COMMENT 'content or non_content based on namespace being in content space or not',
  `other_tags`           array<string> COMMENT 'Can contain: deleted (and deleted_day, deleted_month, deleted_year if deleted within the given time period), revetered and revert (for revisions), self_created (for users), user_first_24_hours if a revision is made during the first 24 hours of a user registration, redirect (for pages)',
  `text_bytes_diff`      bigint COMMENT 'The text-bytes difference of the event (or sum in case of digests)',
  `text_bytes_diff_abs`  bigint COMMENT 'The absolute value of text-bytes difference for the event (or sum in case of digests)',
  `revisions`            bigint COMMENT '1 if the event is entity revision, or sum of revisions in case of digests'
)

PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular labs imports)'
)

STORED AS PARQUET

LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki/history_reduced';
