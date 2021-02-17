-- Create table statement for session_length_daily table.
--
-- This is a daily intermediate representation of the session length data set.
-- Each row corresponds to a session and has 2 fields: wiki, which indicates
-- the wiki domain (without the .org); and session_length, which indicates the
-- length of the session in full minutes, rounding down. Note this data might be
-- sampled with dynamic sampling rates, so it's not suited for session counts.
-- It is much smaller than its initial representation (raw data lives in the
-- event.mediawiki_client_session_tick table) and allows for analytical queries,
-- like percentile approximations, and is small enough to be queried by Superset
-- via Presto. For more info, see: /oozie/session_length/daily/README.md
--
-- Usage:
--     hive -f create_session_length_daily_table.hql --database wmf

CREATE EXTERNAL TABLE IF NOT EXISTS `session_length_daily`(
    `wiki`              string  COMMENT 'Wiki domain, without the .org (i.e. en.wikipedia).',
    `session_length`    int     COMMENT 'Length of the session, in minutes, rounded down.'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year',
    `month`             int     COMMENT 'Unpadded month',
    `day`               int     COMMENT 'Unpadded day'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/session_length/daily'
;
