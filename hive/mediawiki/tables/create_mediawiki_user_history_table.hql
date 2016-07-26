DROP TABLE `wmf.mediawiki_user_history`
;
CREATE EXTERNAL TABLE `wmf.mediawiki_user_history`(
    wiki_db                         string          COMMENT 'enwiki, dewiki, eswiktionary, etc.',
    user_id                         bigint          COMMENT 'ID of the user, as in the user table.',
    user_name                       string          COMMENT 'Historical user name.',
    user_name_latest                string          COMMENT 'User name as of today.',
    user_groups                     array<string>   COMMENT 'Historical user groups.',
    user_groups_latest              array<string>   COMMENT 'User groups as of today.',
    user_blocks                     array<string>   COMMENT 'Historical user blocks.',
    user_blocks_latest              array<string>   COMMENT 'User blocks as of today.',
    user_registration_timestamp     string          COMMENT 'When the user accoung was registered, in YYYYMMDDHHmmss format.',
    created_by_self                 boolean         COMMENT 'Whether the user created their own account',
    created_by_system               boolean         COMMENT 'Whether the user account was created by mediawiki (eg. centralauth)',
    created_by_peer                 boolean         COMMENT 'Whether the user account was created by another user',
    anonymous                       boolean         COMMENT 'Whether the user is not registered',
    is_bot_by_name                  boolean         COMMENT 'Whether the user\'s name matches patterns we use to identify bots',
    start_timestamp                 string          COMMENT 'Timestamp from where this state applies (inclusive).',
    end_timestamp                   string          COMMENT 'Timestamp to where this state applies (exclusive).',
    caused_by_event_type            string          COMMENT 'Event that caused this state (create, move, delete or restore).',
    caused_by_user_id               bigint          COMMENT 'ID from the user that caused this state.',
    caused_by_block_expiration      string          COMMENT 'Block expiration timestamp, if any.',
    inferred_from                   string          COMMENT 'If non-NULL, indicates that some of this state\'s fields have been inferred after an inconsistency in the source data.'
)
COMMENT
  'See most up to date documentation at https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Mediawiki_user_history'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki/user_history'
;

-- find all partitons, per http://stackoverflow.com/a/35834372/180664
MSCK REPAIR TABLE `wmf.mediawiki_user_history`
;
