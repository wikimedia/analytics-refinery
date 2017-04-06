-- Generate json formatted mediawiki history to be loaded in Druid
--
-- REMARK: Booleans are converted to 0/1 integers to allow
-- using them both as dimensions and metrics in druid (having
-- them as metrics means for instance counting number of
-- deleted pages)
--
-- Usage:
--     hive -f generate_json_mediawiki_history.hql \
--         -d source_table=wmf.mediawiki_history \
--         -d destination_directory=/tmp/druid/mediawiki_history \
--         -d snapshot=2017-03
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS `tmp_druid_mediawiki_history`;


CREATE EXTERNAL TABLE IF NOT EXISTS `tmp_druid_mediawiki_history` (
  `wiki_db`                                       string        COMMENT 'enwiki, dewiki, eswiktionary, etc.',
  `event_entity`                                  string        COMMENT 'revision, user or page',
  `event_type`                                    string        COMMENT 'create, move, delete, etc.  Detailed explanation in the docs under #Event_types',
  `event_timestamp`                               string        COMMENT 'When this event ocurred, in YYYYMMDDHHmmss format',
  `event_comment`                                 string        COMMENT 'Comment related to this event, sourced from log_comment, rev_comment, etc.',
  `event_user_id`                                 bigint        COMMENT 'Id of the user that caused the event',
  `event_user_text`                               string        COMMENT 'Historical text of the user that caused the event',
  `event_user_text_latest`                        string        COMMENT 'Current text of the user that caused the event',
  `event_user_blocks`                             array<string> COMMENT 'Historical blocks of the user that caused the event',
  `event_user_blocks_latest`                      array<string> COMMENT 'Current blocks of the user that caused the event',
  `event_user_groups`                             array<string> COMMENT 'Historical groups of the user that caused the event',
  `event_user_groups_latest`                      array<string> COMMENT 'Current groups of the user that caused the event',
  `event_user_is_created_by_self`                 int           COMMENT 'Whether the event_user created their own account',
  `event_user_is_created_by_system`               int           COMMENT 'Whether the event_user account was created by mediawiki (eg. centralauth)',
  `event_user_is_created_by_peer`                 int           COMMENT 'Whether the event_user account was created by another user',
  `event_user_is_anonymous`                       int           COMMENT 'Whether the event_user is not registered',
  `event_user_is_bot_by_name`                     int           COMMENT 'Whether the event_user\'s name matches patterns we use to identify bots',
  `event_user_creation_timestamp`                 string        COMMENT 'Registration timestamp of the user that caused the event',

  `page_id`                                       bigint        COMMENT 'In revision/page events: id of the page',
  `page_title`                                    string        COMMENT 'In revision/page events: historical title of the page',
  `page_title_latest`                             string        COMMENT 'In revision/page events: current title of the page',
  `page_namespace`                                int           COMMENT 'In revision/page events: historical namespace of the page.',
  `page_namespace_is_content`                     int           COMMENT 'In revision/page events: historical namespace of the page is categorized as content',
  `page_namespace_latest`                         int           COMMENT 'In revision/page events: current namespace of the page',
  `page_namespace_is_content_latest`              int           COMMENT 'In revision/page events: current namespace of the page is categorized as content',
  `page_is_redirect_latest`                       int           COMMENT 'In revision/page events: whether the page is currently a redirect',
  `page_creation_timestamp`                       string        COMMENT 'In revision/page events: creation timestamp of the page',

  `user_id`                                       bigint        COMMENT 'In user events: id of the user',
  `user_text`                                     string        COMMENT 'In user events: historical user text',
  `user_text_latest`                              string        COMMENT 'In user events: current user text',
  `user_blocks`                                   array<string> COMMENT 'In user events: historical user blocks',
  `user_blocks_latest`                            array<string> COMMENT 'In user events: current user blocks',
  `user_groups`                                   array<string> COMMENT 'In user events: historical user groups',
  `user_groups_latest`                            array<string> COMMENT 'In user events: current user groups',
  `user_is_created_by_self`                       int           COMMENT 'In user events: whether the user created their own account',
  `user_is_created_by_system`                     int           COMMENT 'In user events: whether the user account was created by mediawiki',
  `user_is_created_by_peer`                       int           COMMENT 'In user events: whether the user account was created by another user',
  `user_is_anonymous`                             int           COMMENT 'In user events: whether the user is not registered',
  `user_is_bot_by_name`                           int           COMMENT 'In user events: whether the user\'s name matches patterns we use to identify bots',
  `user_creation_timestamp`                       string        COMMENT 'In user events: registration timestamp of the user.',

  `revision_id`                                   bigint        COMMENT 'In revision events: id of the revision',
  `revision_parent_id`                            bigint        COMMENT 'In revision events: id of the parent revision',
  `revision_minor_edit`                           int           COMMENT 'In revision events: whether it is a minor edit or not',
  `revision_text_bytes`                           bigint        COMMENT 'In revision events: number of bytes of revision',
  `revision_text_bytes_diff`                      bigint        COMMENT 'In revision events: change in bytes relative to parent revision (can be negative).',
  `revision_text_sha1`                            string        COMMENT 'In revision events: sha1 hash of the revision',
  `revision_content_model`                        string        COMMENT 'In revision events: content model of revision',
  `revision_content_format`                       string        COMMENT 'In revision events: content format of revision',
  `revision_is_deleted`                           int           COMMENT 'In revision events: whether this revision has been deleted (moved to archive table)',
  `revision_deleted_timestamp`                    string        COMMENT 'In revision events: the timestamp when the revision was deleted',
  `revision_is_identity_reverted`                 int           COMMENT 'In revision events: whether this revision was reverted by another future revision',
  `revision_first_identity_reverting_revision_id` bigint        COMMENT 'In revision events: id of the revision that reverted this revision',
  `revision_seconds_to_identity_revert`              bigint     COMMENT 'In revision events: seconds elapsed between revision posting and its revert (if there was one)',
  `revision_is_identity_revert`                   int           COMMENT 'In revision events: whether this revision reverts other revisions'
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';


INSERT OVERWRITE TABLE tmp_druid_mediawiki_history
SELECT
    wiki_db,
    event_entity,
    event_type,
    event_timestamp,
    event_comment,
    event_user_id,
    event_user_text,
    event_user_text_latest,
    event_user_blocks,
    event_user_blocks_latest,
    event_user_groups,
    event_user_groups_latest,
    CASE WHEN event_user_is_created_by_self THEN 1 ELSE 0 END AS event_user_is_created_by_self,
    CASE WHEN event_user_is_created_by_system THEN 1 ELSE 0 END AS event_user_is_created_by_system,
    CASE WHEN event_user_is_created_by_peer THEN 1 ELSE 0 END AS event_user_is_created_by_peer,
    CASE WHEN event_user_is_anonymous THEN 1 ELSE 0 END AS event_user_is_anonymous,
    CASE WHEN event_user_is_bot_by_name THEN 1 ELSE 0 END AS event_user_is_bot_by_name,
    event_user_creation_timestamp,

    page_id,
    page_title,
    page_title_latest,
    page_namespace,
    CASE WHEN page_namespace_is_content THEN 1 ELSE 0 END AS page_namespace_is_content,
    page_namespace_latest,
    CASE WHEN page_namespace_is_content_latest THEN 1 ELSE 0 END AS page_namespace_is_content_latest,
    CASE WHEN page_is_redirect_latest THEN 1 ELSE 0 END AS page_is_redirect_latest,
    page_creation_timestamp,

    user_id,
    user_text,
    user_text_latest,
    user_blocks,
    user_blocks_latest,
    user_groups,
    user_groups_latest,
    CASE WHEN user_is_created_by_self THEN 1 ELSE 0 END AS user_is_created_by_self,
    CASE WHEN user_is_created_by_system THEN 1 ELSE 0 END AS user_is_created_by_system,
    CASE WHEN user_is_created_by_peer THEN 1 ELSE 0 END AS user_is_created_by_peer,
    CASE WHEN user_is_anonymous THEN 1 ELSE 0 END AS user_is_anonymous,
    CASE WHEN user_is_bot_by_name THEN 1 ELSE 0 END AS user_is_bot_by_name,
    user_creation_timestamp,

    revision_id,
    revision_parent_id,
    CASE WHEN revision_minor_edit THEN 1 ELSE 0 END AS revision_minor_edit,
    revision_text_bytes,
    revision_text_bytes_diff,
    revision_text_sha1,
    revision_content_model,
    revision_content_format,
    CASE WHEN revision_is_deleted THEN 1 ELSE 0 END AS revision_is_deleted,
    revision_deleted_timestamp,
    CASE WHEN revision_is_identity_reverted THEN 1 ELSE 0 END AS revision_is_identity_reverted,
    revision_first_identity_reverting_revision_id,
    revision_seconds_to_identity_revert,
    CASE WHEN revision_is_identity_revert THEN 1 ELSE 0 END AS revision_is_identity_revert
FROM ${source_table}
WHERE TRUE
    AND snapshot = '${snapshot}'
    -- Only export rows with valid timestamp format
    AND event_timestamp IS NOT NULL
    AND LENGTH(event_timestamp) = 14
;


DROP TABLE IF EXISTS tmp_druid_mediawiki_history;
