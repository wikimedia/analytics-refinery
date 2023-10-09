-- Prepares a CSV dataset containing one day of referer data to be archived
--
-- Usage:
-- spark3-sql -f compute_referer_archive_daily.hql                              \
--            -d referer_archive_source_table=wmf_traffic.referrer_daily        \
--            -d destination_directory=/tmp/archive_test                        \
--            -d day=2021-03-03                                                 \
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
    DATE_FORMAT(day, 'yyyyMMdd') AS day

FROM ${referer_archive_source_table}
WHERE day = '${day}'
ORDER BY country;
