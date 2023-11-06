-- Creates table statement for mediawiki_wikitext_history table.
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
--     hive -f create_mediawiki_wikitext_history_table.hql \
--         --database wmf
--


CREATE EXTERNAL TABLE `mediawiki_wikitext_history`(
  `page_id`                                       bigint        COMMENT 'id of the page',
  `page_namespace`                                int           COMMENT 'namespace of the page',
  `page_title`                                    string        COMMENT 'title of the page',
  `page_redirect_title`                           string        COMMENT 'title of the redirected-to page',
  `page_restrictions`                             array<string> COMMENT 'restrictions of the page',

  `user_id`                                       bigint        COMMENT 'id of the user that made the revision; null if anonymous, zero if old system user, and -1 when deleted or malformed XML was imported',
  `user_text`                                     string        COMMENT 'text of the user that made the revision (either username or IP)',

  `revision_id`                                   bigint        COMMENT 'id of the revision',
  `revision_parent_id`                            bigint        COMMENT 'id of the parent revision, null when this is the first revision in the chain',
  --`revision_timestamp`                          timestamp     COMMENT 'timestamp of the revision',
  `revision_timestamp`                            string        COMMENT 'timestamp of the revision (ISO8601 format)',
  `revision_minor_edit`                           boolean       COMMENT 'whether this revision is a minor edit or not',
  `revision_comment`                              string        COMMENT 'Comment made with revision',
  `revision_text_bytes`                           bigint        COMMENT 'bytes number of the revision text',
  `revision_text_sha1`                            string        COMMENT 'sha1 hash of the revision text',
  `revision_text`                                 string        COMMENT 'text of the revision',
  `revision_content_model`                        string        COMMENT 'content model of the revision',
  `revision_content_format`                       string        COMMENT 'content format of the revision',

  `user_is_visible`                               boolean       COMMENT 'true if this revision has not had its user deleted via rev_deleted',
  `comment_is_visible`                            boolean       COMMENT 'true if this revision has not had its comment deleted via rev_deleted',
  `content_is_visible`                            boolean       COMMENT 'true if this revision has not had its text content deleted via rev_deleted'
)
COMMENT
  'See most up to date documentation at https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Mediawiki_wikitext_history'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular imports)',
  `wiki_db` string COMMENT 'The wiki_db project')
STORED AS AVRO
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki/wikitext/history'
;
