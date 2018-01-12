-- Checks for false-positive when data-loss occurs in webrequest.
--
-- More precisely, verify that missing (host, sequence-number) for a given
-- hour are found in the next hour.
--
-- WARNING: This script is to be run when the hour next to the one
--          raising alarms has been refined.
--
-- Sample output:
--
-- ch.hostname missing_start missing_end missing_count false_positive
-- cp1054.eqiad.wmnet  2418100933  2418100933  1 true
-- cp1065.eqiad.wmnet  2357768835  2358330425  561591  true
-- cp3031.esams.wmnet  6893052295  6893052295  1 true
-- cp3033.esams.wmnet  7930445930  7930446098  169 true
-- cp4032.ulsfo.wmnet  2757460661  2757460809  149 true
--
-- If the last column, named false_positive, is true, then no is actually lost.
--
-- Parameters:
--     table_name        -- Fully qualified table name to look for dataloss
--                          false-positives
--     webrequest_source -- webrequest_source of partition to look for dataloss
--                          false-positives
--     year              -- year of partition to look for dataloss
--                          false-positives
--     month             -- month of partition to look for dataloss
--                          false-positives
--     day               -- day of partition to look for dataloss
--                          false-positives
--     hour              -- hour of partition to look for dataloss
--                          false-positives
--
--
-- Usage:
--     hive -f check_dataloss_false_positives.hql \
--         -d table_name=wmf.webrequest           \
--         -d webrequest_source=text              \
--         -d year=2018                           \
--         -d month=1                             \
--         -d day=8                               \
--         -d hour=4
--


WITH
  current_hour AS (
    SELECT
        hostname,
        sequence + 1 AS missing_start,
        next_sequence - 1 AS missing_end,
        next_sequence - sequence - 1 AS missing_count,
        next_sequence AS to_match
    FROM (
        SELECT
            hostname,
            sequence,
            LEAD(sequence) OVER (
                PARTITION BY hostname ORDER BY sequence ASC
            ) AS next_sequence
        FROM ${table_name}
        WHERE webrequest_source='${webrequest_source}'
            AND year=${year}
            AND month=${month}
            AND day=${day}
            AND hour=${hour}
    ) webrequest_with_next_sequence
    WHERE
        next_sequence IS NOT NULL -- This condition drops the maximum
            -- sequence number per partition per host. LEAD yields NULL
            -- for those.
        AND next_sequence != sequence + 1 -- This condition drops the rows
            -- for which the next sequence number is as expected.
  ),

    next_hour AS (
    SELECT
        hostname,
        sequence + 1 AS to_match
    FROM (
        SELECT
            hostname,
            sequence,
            LEAD(sequence) OVER (
                PARTITION BY hostname ORDER BY sequence ASC
            ) AS next_sequence
        FROM ${table_name}
        WHERE webrequest_source='${webrequest_source}'
            AND year=${year}
            AND month=${month}
            AND day=(${day} + ((${hour} + 1) / 24))
            AND hour=((${hour} + 1) % 24)
    ) webrequest_with_next_sequence
    WHERE
        next_sequence IS NOT NULL -- This condition drops the maximum
            -- sequence number per partition per host. LEAD yields NULL
            -- for those.
        AND next_sequence != sequence + 1 -- This condition drops the rows
            -- for which the next sequence number is as expected.
  )

SELECT
    ch.hostname,
    missing_start,
    missing_end,
    missing_count,
    (ch.to_match IS NOT NULL) AS false_positive
FROM current_hour ch
  LEFT JOIN next_hour nh
    ON (ch.hostname = nh.hostname
      AND ch.to_match = nh.to_match)
WHERE nh.to_match IS NULL
ORDER BY ch.hostname, missing_start
LIMIT 10000000
;
