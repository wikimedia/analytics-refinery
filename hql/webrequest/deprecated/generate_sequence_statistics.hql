-- Selects stats about sequence numbers in the webrequest
-- table grouped by hostname and inserts them into a table.
--
-- These stats can be queried to infer how complete a
-- certain host's webrequest logs are for a certain hour.
-- If percent_different == 0.0, then most likely all is
-- well.  If percent_different < 0.0, then there has likely
-- been loss of data.  If percent_different > 0.0, then there
-- has likely been duplication of data.
--
-- This query does not actually check for individual missing or
-- duplicate requests.  To differentiate between these cases,
-- check the value of count_duplicate.
--   - If count_duplicate is 0, there are no duplicates, but there
--     might be holes, which we can check by looking at count_different.
--   - If count_duplicate is > 0, there are that many duplicates.
--   - If count_duplicate is < 0, something is broken :-)
--
-- Note that statistics do not consider records with a missing timestamp
-- (dt='-'), because they may break sequence numbers and cause false alarms.
-- The column count_incomplete indicates how many records have an
-- undefined timestamp (dt='-'), so that subsequent jobs can alert on that.
--
-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          statistics for.
--     destination_table -- Fully qualified table name to store the
--                          computed statistics in. This table should
--                          have schema described in [1].
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
-- [1] hql/webrequest/create_webrequest_sequence_stats_table.hql
--
-- This job does not need much memory, but it needs to read the source files 2 times. Thus the parallelization should
-- match at least 2 times the number of files (txt.gz, not splittable for read)
--
-- Usage example:
--     spark3-sql \
--         --master yarn \
--         --executor-memory 4G \
--         --executor-cores 4 \
--         --driver-memory 2G \
--         --driver-cores 1 \
--         --conf spark.dynamicAllocation.maxExecutors=128 \
--         --name test-generate-sequence-statistics \
--         -f generate_sequence_statistics.hql \
--         -d source_table=wmf_raw.webrequest \
--         -d destination_table=user.webrequest_sequence_stats \
--         -d webrequest_source=text \
--         -d year=2018 \
--         -d month=5 \
--         -d day=12 \
--         -d hour=1

WITH
    statistics AS (
        SELECT
            hostname,
            MIN(sequence)                                                  AS sequence_min,
            MAX(sequence)                                                  AS sequence_max,
            COUNT(*)                                                       AS count_actual,
            MAX(sequence) - MIN(sequence) + 1                              AS count_expected,
            MAX(sequence) - MIN(sequence) + 1 - COUNT(*)                   AS count_different,
            COUNT(*) - COUNT(DISTINCT sequence)                            AS count_duplicate,
            SUM(if(sequence IS NULL,1,0))                                  AS count_null_sequence,
            ((COUNT(*) / (MAX(sequence) - MIN(sequence) + 1)) - 1) * 100.0 AS percent_different
        FROM
            ${source_table}
        WHERE
            webrequest_source='${webrequest_source}' AND
            year=${year} AND month=${month} AND day=${day} AND hour=${hour} AND
            dt != '-'
        GROUP BY
            hostname, webrequest_source, year, month, day, hour
    ),

    undefined AS (
        SELECT
            hostname,
            SUM(IF(dt='-',1,0)) AS count_incomplete
        FROM
            ${source_table}
        WHERE
            webrequest_source='${webrequest_source}' AND
            year=${year} AND month=${month} AND day=${day} AND hour=${hour}
        GROUP BY
            hostname, webrequest_source, year, month, day, hour
    )

INSERT OVERWRITE TABLE ${destination_table}
PARTITION (
    webrequest_source='${webrequest_source}',
    year=${year},
    month=${month},
    day=${day},
    hour=${hour}
)
SELECT /*+ COALESCE(1) */
    undefined.hostname,
    sequence_min,
    sequence_max,
    count_actual,
    count_expected,
    count_different,
    count_duplicate,
    count_null_sequence,
    percent_different,
    count_incomplete
FROM statistics
RIGHT JOIN undefined USING(hostname)
;
