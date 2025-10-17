-- Creates an Iceberg table that stores daily counts of pageviews per user and page
--
-- Daily Pageviews per editor is defined as:
--   The number of page views each editor's edited pages received on a given day.
--   Any page an editor has ever edited on or before the given day is counted as
--   an editor's edited pages.
--
-- NOTE: This table only includes data about real user account editors.
--       No IP or Temp editors here.
--
-- Some of the fields stored here are mutable, e.g. user_name, user_is_bot, etc.
-- Since this table is an aggregation, we have to choose a value for the mutable fields that
-- makes the most sense for the aggregation granularity.  The load queries lookup the latest
-- value for the mutable fields at the time of the latest edit per user and page on the day.
-- NOTE: we wanted to include page mutable fields here, but pageview_hourly does not have
-- a consistent association of page_id with page_title, etc.
-- See: https://phabricator.wikimedia.org/T408798
--
-- Data size estimate:
--  2025-10-25's day worth of data was < 30,000,000 rows.
--  At an estimated record size of 150 bytes per row,
--  this table will grow about ~4GB per year.
--  (according to the back of the napkin)

-- Parameters:
--     destination_table
--         Database and name under which the created
--         table will be registered
--
--     location
--         Directory location in which to store the data.
--
-- Usage
--     spark3-sql -f create_pageview_per_editor_per_page_daily.hql \
--                -d destination_table=wmf_readership.pageview_per_editor_per_page_daily \
--                -d location=hdfs://analytics-hadoop/wmf/data/wmf_readership/pageview_per_editor_per_page_daily
--

CREATE TABLE IF NOT EXISTS ${destination_table} (
    `day`               DATE,
    `user_central_id`   BIGINT  COMMENT 'MediaWiki user central ID. At Wikimedia, this is the CentralAuth globaluser gu_id field.',
    `user_id`           BIGINT  COMMENT 'Local wiki user ID. Inclued for convience, you should use user_central_id.',
    `user_name`         STRING  COMMENT 'User name on this day. Included for conveneince; user_central_id is canonical. The value here should match the value at the time this user last edited this page.',
    `user_is_bot`       BOOLEAN COMMENT 'True if this user is considered to be a bot on this day. This is originally determined via the MediaWiki $user->isBot() method, which considers both user_groups and user permissions. The value here should match the value at the time this user last edited this page',
    `user_is_system`    BOOLEAN COMMENT 'True if the user is a MediaWiki system user. These are users that cannot authenticate. These are usually listed in ReservedUsernames. The value here should match the value at the time this user last edited this page',
    `wiki_id`           STRING  COMMENT 'The wiki ID.',
    `wiki`              STRING  COMMENT 'The canonical wiki domain name, e.g. en.wikipedia.org, commons.wikimedia.org, etc.',
    `pageview_project`  STRING  COMMENT 'DEPRECATED: The TLD-less project name of the wiki, e.g. en.wikipedia, commons.wikimedia, etc. This field is included purely for easier use with pageviews_hourly.',
    `page_id`           BIGINT  COMMENT 'The page ID.',
    -- NOTE: we wanted to include page_namespace_id and page_title here, but pageview_hourly's
    -- association of these with page_id is not consistent
    -- See: https://phabricator.wikimedia.org/T408798
    `view_count`        BIGINT  COMMENT 'The count of pageviews this page received on this day'
)
USING ICEBERG
PARTITIONED BY (`day`)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd',
    'write.distribution-mode' = 'hash'
)
COMMENT 'Stores daily pageview counts to each editor\'s edited pages. For each day, consider all pages ever edited by each editor. If any of those pages revieved pageviews, the number of pageviews they received will be stored in this table per editor per page. This allows for easier calculation of an editor\'s pageview impact.'
LOCATION '${location}'
;
