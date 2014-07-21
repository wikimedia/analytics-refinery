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
-- ${destination_table} should have schema described in
--     hive/webrequest/create_webrequest_sequence_stats_table.hql
--
-- Usage:
--   hive \
--     -f sequence_stats.hql                              \
--     -d source_table=wmf_raw.webrequest                 \
--     -d destination_table=wmf.webrequest_sequence_stats \
--     -d webrequest_source=bits                          \
--     -d year=2014                                       \
--     -d month=5                                         \
--     -d day=12                                          \
--     -d hour=1

INSERT OVERWRITE TABLE ${destination_table}
  PARTITION(webrequest_source='${webrequest_source}',year=${year},month=${month},day=${day},hour=${hour})
  SELECT
    hostname,
    MIN(sequence)                                                  AS sequence_min,
    MAX(sequence)                                                  AS sequence_max,
    COUNT(*)                                                       AS count_actual,
    MAX(sequence) - MIN(sequence) + 1                              AS count_expected,
    MAX(sequence) - MIN(sequence) + 1 - COUNT(hostname)            AS count_different,
    COUNT(*) - COUNT(DISTINCT sequence)                            AS count_duplicate,
    SUM(if(sequence IS NULL,1,0))                                  AS count_null_sequence,
    ((COUNT(*) / (MAX(sequence) - MIN(sequence) + 1)) - 1) * 100.0 AS percent_different
  FROM
    ${source_table}
  WHERE
    webrequest_source='${webrequest_source}' AND
    year=${year} AND month=${month} AND day=${day} AND hour=${hour}
  GROUP BY
    hostname, webrequest_source, year, month, day, hour;