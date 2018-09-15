-- Aggregates statsistics from the webrequest_sequence_stats table across all
-- webrequest originating servers for an hourly partition.
--
-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          statistics for.  This is probably
--                          'webrequest_sequence_stats'.
--     destination_table -- Fully qualified table name to stopre the
--                          computed statistics in. This table should
--                          have schema described in [2].
--     webrequest_source -- webrequest_source of partition to compute
--                          statistics for.
--     year              -- year of partition to compute statistics
--                          for.
--     month             -- month of partition to compute statistics
--                          for.
--     day               -- day of partition to compute statistics
--                          for.
--     hour              -- hour of partition to compute statistics
--                          for.
--
-- [1] hive/webrequest/create_webrequest_sequence_stats_hourly_table.hql
--
-- Usage:
--     hive -f generate_sequence_statistics_hourly.hql                   \
--         -d source_table=wmf_raw.webrequest_sequence_stats             \
--         -d destination_table=wmf_raw.webrequest_sequence_stats_hourly \
--         -d webrequest_source=text                                     \
--         -d year=2018                                                  \
--         -d month=8                                                    \
--         -d day=11                                                     \
--         -d hour=1
--

INSERT OVERWRITE TABLE ${destination_table}
PARTITION(
    webrequest_source='${webrequest_source}',
    year=${year},
    month=${month},
    day=${day},
    hour=${hour}
)
SELECT
    count_actual,
    count_expected,
    count_null_sequence,
    count_duplicate,
    count_lost,
    ROUND(((count_duplicate / count_expected) * 100.0), 8)  AS percent_duplicate,
    ROUND(((count_lost      / count_expected) * 100.0), 8)  AS percent_lost,
    count_incomplete
FROM (
SELECT
    SUM(count_actual)                                       AS count_actual,
    SUM(count_expected)                                     AS count_expected,
    SUM(count_null_sequence)                                AS count_null_sequence,
    SUM(count_duplicate)                                    AS count_duplicate,
    SUM(count_different) + SUM(count_duplicate)             AS count_lost,
    SUM(COALESCE(count_incomplete, 0))                      AS count_incomplete
FROM
    ${source_table}
WHERE
    webrequest_source='${webrequest_source}' AND
    year=${year} AND month=${month} AND day=${day} AND hour=${hour}
    -- sequence_min == 0 means varnishkafka restarted.
    -- Even though it skews results, don't include hosts
    -- with reset seqeuence numbers in these results, as
    -- they are a common cause of false postives in percent_loss and
    -- percent_duplicate.
    AND sequence_min <> 0
GROUP BY
    webrequest_source, year, month, day, hour
) a;
