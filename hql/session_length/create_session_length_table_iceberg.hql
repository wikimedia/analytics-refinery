-- Create table statement for session length Iceberg table.
--
-- This is a daily intermediate representation of the session length data set.
-- Each row has 3 fields:
--   * wiki, which indicates the wiki domain (without the .org);
--   * session_length, which indicates the length of the session in full minutes,
--     rounding down;
--   * and session_count, which indicates the aggregated count of sessions that
--   share the same wiki and session_length values.
--
-- The session_count field can be used as weight for percentile calculations.
-- The table is much smaller than its initial representation (raw data lives
-- in the event.mediawiki_client_session_tick table), allows for analytical
-- queries, and is small enough to be queried by Superset via Presto.
-- For more info, see: /hql/session_length/README.md

-- Usage
--     spark3-sql
--       -f create_session_length_table_iceberg.sql \
--       --database wmf_traffic \
--       -d location=/wmf/data/wmf_traffic/session_length
--

CREATE EXTERNAL TABLE IF NOT EXISTS `session_length` (
    `wiki`           string  COMMENT 'Wiki domain, without the .org (i.e. en.wikipedia).',
    `session_length` int     COMMENT 'Length of the session, in minutes, rounded down.',
    `session_count`  int     COMMENT 'Number of sessions for this wiki and session_length.',
    `day`            date    COMMENT 'The date of measurement.'
)
USING ICEBERG
LOCATION '${location}'
;
