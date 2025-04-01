-- Create table statement for sequence number statistics for the raw webrequest table.
--
-- Parameters:
--     database: should be wmf_raw
--
-- Usage
--     spark3-sql -f create_webrequest_sequence_stats_table.hql \
--       --database user1
--

CREATE TABLE `webrequest_sequence_stats`(
    `hostname`            string  COMMENT 'Source node hostname',
    `sequence_min`        bigint  COMMENT 'Min sequence found for this hostname in this hour',
    `sequence_max`        bigint  COMMENT 'Max sequence found for this hostname in this hour',
    `count_actual`        bigint  COMMENT 'Actual number of records for this hostname in this hour',
    `count_expected`      bigint  COMMENT 'Max - Min + 1.  All is well if this == count_actual',
    `count_different`     bigint  COMMENT 'count_expected - count_actual',
    `count_duplicate`     bigint  COMMENT 'Number of duplicate sequences for this hostname in this hour',
    `count_null_sequence` bigint  COMMENT 'Sanity check for number of records where sequence is NULL.',
    `percent_different`   double  COMMENT 'Difference in percent between count_expected and count_actual.',
    `count_incomplete`    bigint  COMMENT 'Number of records missing critical fields, probably due to logging errors from the caching-traffic layer'
)
PARTITIONED BY (
    `webrequest_source`   string  COMMENT 'Source cluster',
    `year`                int,
    `month`               int,
    `day`                 int,
    `hour`                int
)
;
