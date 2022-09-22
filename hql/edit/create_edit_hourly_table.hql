e- Creates a table to store edit_hourly data.
-- See: oozie/edit/hourly/README.md
--
-- Usage
--     hive -f create_edit_hourly_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `edit_hourly`(
    `ts`                      string         COMMENT 'Timestamp string truncated to the hour. Format: "YYYY-MM-DD HH:00:00.0".',
    `project`                 string         COMMENT 'Project name, i.e.: "zu.wikibooks".',
    `project_family`          string         COMMENT 'Project name, i.e.: "wikibooks".',
    `language`                string         COMMENT 'Project language, i.e.: Zulu.',
    `user_is_anonymous`       boolean        COMMENT 'Whether user is anonymous or not.',
    `user_is_bot`             boolean        COMMENT 'Whether user is bot or not.',
    `user_is_administrator`   boolean        COMMENT 'Whether user is administrator or not.',
    `user_groups`             array<string>  COMMENT 'User groups array.',
    `user_tenure_bucket`      string         COMMENT 'Bucketed time between user creation and edit (Under 1 day, 1 to 7 days, 7 to 30 days, ..., Over 10 years, Undefined).',
    `namespace_is_content`    boolean        COMMENT 'Whether the namespace is of type content or not.',
    `namespace_is_talk`       boolean        COMMENT 'Whether the namespace is of type talk or not.',
    `namespace_name`          string         COMMENT 'Namespace name (Main, Talk, User, User talk, etc.). See: oozie/edit/hourly/edit_hourly.hql.',
    `namespace_id`            int            COMMENT 'Namespace id.',
    `creates_new_page`        boolean        COMMENT 'Whether the edit was the first of a page (page creation).',
    `is_deleted`              boolean        COMMENT 'Whether the edit has been deleted.',
    `is_reverted`             boolean        COMMENT 'Whether the edit has been reverted.',
    `is_redirect_currently`   boolean        COMMENT 'Whether the page is *currently* a redirect (no historical information available)',
    `user_edit_count_bucket`  string         COMMENT 'Authors edit count bucket (1-4, 5-99, 100-999, 1000-9999, 10000+).',
    `platform`                string         COMMENT 'Access method (iOS, Android, Mobile web, Other).',
    `interface`               string         COMMENT 'Editing interface (VisualEditor, 2017 wikitext editor, Switched from VisualEditor to wikitext editor, Other).',
    `revision_tags`           array<string>  COMMENT 'Revision tags (change tags) array.',
    `edit_count`              bigint         COMMENT 'Number of edits belonging to this hourly bucket (for the given dimension value set).',
    `text_bytes_diff`         bigint         COMMENT 'Number of bytes added minus number of bytes removed belonging to this hourly bucket (for the given dimension value set).'
)
PARTITIONED BY (
    `snapshot`                string         COMMENT 'MediaWiki history snapshot (YYYY-MM format).'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/edit/hourly'
;
