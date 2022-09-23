-- Creates table statement for editors_daily table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_editors_daily_table.hql   \
--          --database wmf
--

CREATE EXTERNAL TABLE `editors_daily` (
  `wiki_db`                     string      COMMENT 'The wiki database of origin',
  `country_code`                string      COMMENT 'The 2-letter ISO country code this group of actions geolocated to, including Unknown (--)',
  `user_fingerprint_or_name`      string      COMMENT 'If an anonymous user, this is a hash of the IP + UA, otherwise it is their global username across wiki dbs',
  `user_is_anonymous`           boolean     COMMENT 'Whether or not this user actions were made anonymously',
  `date`                        string      COMMENT 'The YYYY-MM-DD date for this group of actions',
  `edit_count`                  bigint      COMMENT 'The total count of actions for this grouping',
  `namespace_zero_edit_count`   bigint      COMMENT 'The total count of actions to namespace zero for this grouping',
  `network_origin`              string      COMMENT 'The network-origin as computed by GetNetworkOriginUDF (can be Internet, Wikimedia, Wikimedia_labs)',
  `user_is_bot_by`              array<string>  COMMENT 'Whether this user is identified as a bot, values can be: group, name, both or empty',
  `action_type`                 int         COMMENT 'The action type for this group of actions - see https://www.mediawiki.org/wiki/Manual:Recentchanges_table#rc_type'
)
COMMENT
  'NOTE: This table has private location in it.  It should be truncated so no data older than 90 days is available.  See up to date schema documentation for the source table at https://www.mediawiki.org/wiki/Extension:CheckUser/cu_changes_table'
PARTITIONED BY (
  `month` string COMMENT 'The month in YYYY-MM format, all edits from that month from the cu_changes table are aggregated in this partition'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki_private/editors_daily'
;
