-- Prepares a CSV dataset containing one day of referer data to be archived
--
-- Usage:
-- spark3-sql -f compute_archive_daily.hql                                      \
--            -d referer_archive_source_table=wmf.referrer_daily                \
--            -d destination_directory=/tmp/archive_test                        \
--            -d year=2021                                                      \
--            -d month=3                                                        \
--            -d day=3                                                          \
--

INSERT OVERWRITE DIRECTORY '${destination_directory}'
    USING CSV OPTIONS ('sep' = '\t', 'header' = 'true', 'compression'= 'none')
SELECT /*+ COALESCE(1) */
    country,
    lang,
    browser_family,
    os_family,
    search_engine,
    num_referrals,
    CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0")) AS day

FROM ${referer_archive_source_table}
WHERE year = ${year}
  and month = ${month}
  and day = ${day}
ORDER BY country;
