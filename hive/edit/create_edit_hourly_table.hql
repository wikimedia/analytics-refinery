-- Creates a table to store edit_hourly data.
-- See: oozie/edit/hourly/README.md
--
-- Usage
--     hive -f create_edit_hourly_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `edit_hourly`(
    `ts`                      string         COMMENT 'Timestamp string truncated to the hour. Format: "YYYY-MM-DD HH:00:00.0".',
    `project`                 string         COMMENT 'Project name, i.e.: "en.wikipedia".',
    `user_is_anonymous`       boolean        COMMENT 'Whether user is anonymous or not.',
    `user_is_bot`             boolean        COMMENT 'Whether user is bot or not.',
    `user_is_administrator`   boolean        COMMENT 'Whether user is administrator or not.',
    `user_groups`             array<string>  COMMENT 'User groups array.',
    `namespace_is_content`    boolean        COMMENT 'Whether the namespace is of type content or not.',
    `namespace_is_talk`       boolean        COMMENT 'Whether the namespace is of type talk or not.',
    `namespace_name`          string         COMMENT 'Namespace name (Main, Talk, User, User talk, etc.). See: oozie/edit/hourly/edit_hourly.hql.',
    `namespace_id`            int            COMMENT 'Namespace id.',
    `creates_new_page`        boolean        COMMENT 'Whether the edit was the first of a page (page creation).',
    `is_deleted`              boolean        COMMENT 'Whether the edit has been deleted.',
    `is_reverted`             boolean        COMMENT 'Whether the edit has been reverted.',
    `user_edit_count_bucket`  string         COMMENT 'Authors edit count bucket (1-4, 5-99, 100-999, 1000-9999, 10000+).',
    `edit_count`              bigint         COMMENT 'Number of edits belonging to this hourly bucket (for the given dimension value set).',
    `text_bytes_diff`         bigint         COMMENT 'Number of bytes added minus number of bytes removed belonging to this hourly bucket (for the given dimension value set).'
)
PARTITIONED BY (
    `snapshot`                string         COMMENT 'MediaWiki history snapshot (YYYY-MM format).'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/edit/hourly'
;
