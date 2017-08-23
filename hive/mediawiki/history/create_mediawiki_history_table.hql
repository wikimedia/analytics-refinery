-- Creates table statement for mediawiki_history table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_history_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE `mediawiki_history`(
  `wiki_db`                                       string        COMMENT 'enwiki, dewiki, eswiktionary, etc.',
  `event_entity`                                  string        COMMENT 'revision, user or page',
  `event_type`                                    string        COMMENT 'create, move, delete, etc.  Detailed explanation in the docs under #Event_types',
  `event_timestamp`                               timestamp     COMMENT 'When this event ocurred',
  `event_comment`                                 string        COMMENT 'Comment related to this event, sourced from log_comment, rev_comment, etc.',
  `event_user_id`                                 bigint        COMMENT 'Id of the user that caused the event',
  `event_user_text`                               string        COMMENT 'Historical text of the user that caused the event',
  `event_user_text_latest`                        string        COMMENT 'Current text of the user that caused the event',
  `event_user_blocks`                             array<string> COMMENT 'Historical blocks of the user that caused the event',
  `event_user_blocks_latest`                      array<string> COMMENT 'Current blocks of the user that caused the event',
  `event_user_groups`                             array<string> COMMENT 'Historical groups of the user that caused the event',
  `event_user_groups_latest`                      array<string> COMMENT 'Current groups of the user that caused the event',
  `event_user_is_created_by_self`                 boolean       COMMENT 'Whether the event_user created their own account',
  `event_user_is_created_by_system`               boolean       COMMENT 'Whether the event_user account was created by mediawiki (eg. centralauth)',
  `event_user_is_created_by_peer`                 boolean       COMMENT 'Whether the event_user account was created by another user',
  `event_user_is_anonymous`                       boolean       COMMENT 'Whether the event_user is not registered',
  `event_user_is_bot_by_name`                     boolean       COMMENT 'Whether the event_user\'s name matches patterns we use to identify bots',
  `event_user_creation_timestamp`                 timestamp     COMMENT 'Registration timestamp of the user that caused the event',
  `event_user_revision_count`                     bigint        COMMENT 'Cumulative revision count per user for the current event_user_id (only available in revision-create events so far)',
  `event_user_seconds_from_previous_revision`     bigint        COMMENT 'In revision events: seconds elapsed since the previous revision made by the current event_user_id (only available in revision-create events so far)',

  `page_id`                                       bigint        COMMENT 'In revision/page events: id of the page',
  `page_title`                                    string        COMMENT 'In revision/page events: historical title of the page',
  `page_title_latest`                             string        COMMENT 'In revision/page events: current title of the page',
  `page_namespace`                                int           COMMENT 'In revision/page events: historical namespace of the page.',
  `page_namespace_is_content`                     boolean       COMMENT 'In revision/page events: historical namespace of the page is categorized as content',
  `page_namespace_latest`                         int           COMMENT 'In revision/page events: current namespace of the page',
  `page_namespace_is_content_latest`              boolean       COMMENT 'In revision/page events: current namespace of the page is categorized as content',
  `page_is_redirect_latest`                       boolean       COMMENT 'In revision/page events: whether the page is currently a redirect',
  `page_creation_timestamp`                       timestamp     COMMENT 'In revision/page events: creation timestamp of the page',
  `page_revision_count`                           bigint        COMMENT 'In revision/page events: Cumulative revision count per page for the current page_id (only available in revision-create events so far)',
  `page_seconds_from_previous_revision`           bigint        COMMENT 'In revision/page events: seconds elapsed since the previous revision made on the current page_id (only available in revision-create events so far)',

  `user_id`                                       bigint        COMMENT 'In user events: id of the user',
  `user_text`                                     string        COMMENT 'In user events: historical user text',
  `user_text_latest`                              string        COMMENT 'In user events: current user text',
  `user_blocks`                                   array<string> COMMENT 'In user events: historical user blocks',
  `user_blocks_latest`                            array<string> COMMENT 'In user events: current user blocks',
  `user_groups`                                   array<string> COMMENT 'In user events: historical user groups',
  `user_groups_latest`                            array<string> COMMENT 'In user events: current user groups',
  `user_is_created_by_self`                       boolean       COMMENT 'In user events: whether the user created their own account',
  `user_is_created_by_system`                     boolean       COMMENT 'In user events: whether the user account was created by mediawiki',
  `user_is_created_by_peer`                       boolean       COMMENT 'In user events: whether the user account was created by another user',
  `user_is_anonymous`                             boolean       COMMENT 'In user events: whether the user is not registered',
  `user_is_bot_by_name`                           boolean       COMMENT 'In user events: whether the user\'s name matches patterns we use to identify bots',
  `user_creation_timestamp`                       timestamp     COMMENT 'In user events: registration timestamp of the user.',

  `revision_id`                                   bigint        COMMENT 'In revision events: id of the revision',
  `revision_parent_id`                            bigint        COMMENT 'In revision events: id of the parent revision',
  `revision_minor_edit`                           boolean       COMMENT 'In revision events: whether it is a minor edit or not',
  `revision_text_bytes`                           bigint        COMMENT 'In revision events: number of bytes of revision',
  `revision_text_bytes_diff`                      bigint        COMMENT 'In revision events: change in bytes relative to parent revision (can be negative).',
  `revision_text_sha1`                            string        COMMENT 'In revision events: sha1 hash of the revision',
  `revision_content_model`                        string        COMMENT 'In revision events: content model of revision',
  `revision_content_format`                       string        COMMENT 'In revision events: content format of revision',
  `revision_is_deleted`                           boolean       COMMENT 'In revision events: whether this revision has been deleted (moved to archive table)',
  `revision_deleted_timestamp`                    timestamp     COMMENT 'In revision events: the timestamp when the revision was deleted',
  `revision_is_identity_reverted`                 boolean       COMMENT 'In revision events: whether this revision was reverted by another future revision',
  `revision_first_identity_reverting_revision_id` bigint        COMMENT 'In revision events: id of the revision that reverted this revision',
  `revision_seconds_to_identity_revert`           bigint        COMMENT 'In revision events: seconds elapsed between revision posting and its revert (if there was one)',
  `revision_is_identity_revert`                   boolean       COMMENT 'In revision events: whether this revision reverts other revisions'
)
COMMENT
  'See most up to date documentation at https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Mediawiki_history'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular labs imports)')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki/history'
;
