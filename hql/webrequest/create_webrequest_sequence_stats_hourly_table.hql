-- Create table statement for hourly aggregate sequence number statistics for the raw webrequest table.
--
-- Parameters:
--     database: should be wmf_raw
--
-- Usage
--     spark3-sql -f create_webrequest_sequence_stats_hourly_table.hql \
--       --database user1
--

CREATE TABLE `webrequest_sequence_stats_hourly`(
    `count_actual`           bigint COMMENT 'Actual number of records for this hour',
    `count_expected`         bigint COMMENT 'Expected number of records for this hour',
    `count_null_sequence`    bigint COMMENT 'Number of records where sequence is NULL for this hour',
    `count_duplicate`        bigint COMMENT 'Number of duplicate sequences in this hour',
    `count_lost`             bigint COMMENT 'Number of (approximate) missing records in this hour',
    `percent_duplicate`      double COMMENT 'Percent duplicate records in this hour',
    `percent_lost`           double COMMENT 'Percent lost records in this hour',
    `count_incomplete`       bigint COMMENT 'Number of records missing critical fields, probably due to logging errors from the caching-traffic layer'
)
PARTITIONED BY (
    `webrequest_source`   string  COMMENT 'Source cluster',
    `year`                int,
    `month`               int,
    `day`                 int,
    `hour`                int
)
;
