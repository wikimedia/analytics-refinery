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
-- ${destination_table} should have the following schema:
--
--   CREATE TABLE webrequest_sequence_stats(
--     hostname            string  COMMENT 'Source node hostname',
--     webrequest_source   string  COMMENT 'Source cluster',
--     year                int,
--     month               int,
--     day                 int,
--     hour                int,
--     sequence_min        bigint  COMMENT 'Min sequence found for this hostname in this hour',
--     sequence_max        bigint  COMMENT 'Max sequence found for this hostname in this hour',
--     count_actual        bigint  COMMENT 'Actual number of records for this hostname in this hour',
--     count_expected      bigint  COMMENT 'Max - Min + 1.  All is well if this == count_actual',
--     count_different     bigint  COMMENT 'count_expected - count_actual',
--     count_duplicate     bigint  COMMENT 'Number of duplicate sequences for this hostname in this hour',
--     count_null_sequence bigint  COMMENT 'Sanity check for number of records where sequence is NULL.',
--     percent_different   double  COMMENT 'Difference in percent between count_expected and count_actual.'
--   );
--
--
-- Usage:
--   hive --auxpath /usr/lib/hcatalog/share/hcatalog/hcatalog-core-0.5.0-cdh4.3.1.jar \
--     -f sequence_stats.hql                              \
--     -d source_table=wmf.webrequest                     \
--     -d destination_table=wmf.webrequest_sequence_stats \
--     -d year=2014                                       \
--     -d month=05                                        \
--     -d day=12                                          \
--     -d hour=01

INSERT INTO TABLE ${destination_table}
  SELECT
    hostname,
    webrequest_source,
    year,
    month,
    day,
    hour,
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
    year=${year} AND month=${month} AND day=${day} AND hour=${hour}
  GROUP BY
    hostname, webrequest_source, year, month, day, hour;