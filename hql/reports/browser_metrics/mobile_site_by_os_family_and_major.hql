-- Usage
-- spark3-sql -f mobile_site_by_os_family_and_major.hql
--              -d source_table=wmf_traffic.browser_general
--              -d destination_directory=/srv/reportupdater/output/metrics/browser
--              -d end_date=2021-03-19
--

INSERT OVERWRITE DIRECTORY '${destination_directory}'
USING CSV
OPTIONS (
    'sep' '\t',
    'header' 'true',
    'compression' 'none',
    'emptyValue' ''
)
SELECT
    /*+ COALESCE(1) */
    DATE_SUB(day, (DAYOFWEEK(day) - 1)) AS `date`,
    os_family,
    os_major,
    SUM(view_count) AS view_count
FROM ${source_table}
WHERE
    access_method = 'mobile web' AND
    -- Add precise date filtering
    day >= '2015-06-07' AND
    day < '${end_date}'
GROUP BY
    DATE_SUB(day, (DAYOFWEEK(day) - 1)),
    os_family,
    os_major
ORDER BY
    `date`,
    view_count DESC
;