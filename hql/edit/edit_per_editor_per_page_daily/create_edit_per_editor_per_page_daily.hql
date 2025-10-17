-- Creates an Iceberg table to stores a daily count of edits by editors by page
--
-- This table only includes data about real user account editors.
--   No IP or Temp editors here.
--
-- Some of the fields stored here are mutable, e.g. user_name, user_is_bot, page_title, etc.
-- Since this table is an aggregation, we have to choose a value for the mutable fields that
-- makes the most sense for the aggregation granularity.  The load queries lookup the latest
-- value for the mutable fields at the time of the latest edit per user and page on the day.
--
-- Data size estimate:
--  2025-10-25's day worth of data was ~ 1200000 rows.
--  At an estimated record size of 200 bytes per row,
--  this table will grow about ~80GB per year.
--  (according to the back of the napkin)
--
-- Parameters:
--     destination_table
--         Database and name under which the created
--         table will be registered
--
--     location
--         Directory location in which to store the data.
--
-- Usage:
--     spark3-sql -f create_edit_per_editor_per_page_daily.hql \
--                -d destination_table=wmf_contributors.edit_per_editor_per_page_daily \
--                -d location=hdfs://analytics-hadoop/wmf/data/wmf_contributors/edit_per_editor_per_page_daily
--

CREATE TABLE IF NOT EXISTS ${destination_table} (
    `day`               DATE,
    `user_central_id`   BIGINT  COMMENT 'MediaWiki user central ID. At Wikimedia, this is the CentralAuth globaluser gu_id field.',
    `user_id`           BIGINT  COMMENT 'Local wiki user ID. Inclued for convience, you should use user_central_id.',
    `user_name`         STRING  COMMENT 'User name on this day. Included for conveneince; user_central_id is canonical. If this should change on this day, the value here should match the value at the time of the latest edit on this day.',
    `user_is_bot`       BOOLEAN COMMENT 'True if this user is considered to be a bot on this day. This is originally determined via the MediaWiki $user->isBot() method, which considers both user_groups and user permissions. If this should change on this day, the value here should match the value at the time of the latest edit on this day.',
    `user_is_system`    BOOLEAN COMMENT 'True if the user is a MediaWiki system user. These are users that cannot authenticate. These are usually listed in ReservedUsernames. If this should change on this day, the value here should match the value at the time of the latest edit on this day.',
    `wiki_id`           STRING  COMMENT 'The wiki ID, e.g. enwiki, commonswiki,',
    `wiki`              STRING  COMMENT 'The canonical wiki domain name, e.g. en.wikipedia.org, commons.wikimedia.org, etc.',
    `pageview_project`  STRING  COMMENT 'DEPRECATED: The TLD-less project name of the wiki, e.g. en.wikipedia, commons.wikimedia, etc. This field is included purely for easier use with pageviews_hourly.',
    `page_namespace_id` INT     COMMENT 'The namespace of the page. Included for conveneince; page_id is canonical. If this should change on this day, the value here should match the value at the time of the latest edit on this day.',
    `page_id`           BIGINT  COMMENT 'The page ID.',
    `page_title`        STRING  COMMENT 'The normalized page_title at the time of the edit. If this is not namespace 0, this is prefixed by the namespace name. Included for conveneinces; page_id is canonical. If this should change on this day, the value here should match the value at the time of the latest edit on this day.',
    `edit_count`        BIGINT  COMMENT 'The count of edits this user made to this page on this day.'
)
USING ICEBERG
PARTITIONED BY (`day`)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd',
    'write.distribution-mode' = 'hash'
)
COMMENT 'Daily counts of edits to pages by editors with real user accounts. Pages edited by anonymous (IP) or temp users are not stored here.'
LOCATION '${location}'
;
