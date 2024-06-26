-- Usage
-- spark3-sql -f mobile_site_by_browser_family_percent.hql
--              -d source_table=wmf_traffic.browser_general
--              -d destination_directory=/srv/reportupdater/output/metrics/browser
--              -d end_date=2021-03-19
--
WITH
    slice AS (
        SELECT
            DATE_SUB(day, (DAYOFWEEK(day) - 1)) AS weekday,
            *
        FROM ${source_table}
        WHERE
            access_method = 'mobile web' AND
            -- Add actual precise date filtering
            day >= '2015-06-07' AND
            day < '${end_date}'
    ),
    total AS (
        SELECT
            weekday,
            SUM(view_count) AS view_count_total
        FROM slice
        GROUP BY weekday
    )

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
    slice.weekday AS `date`,
    browser_family,
    SUM(view_count) / view_count_total AS percent
FROM slice JOIN total ON slice.weekday=total.weekday
GROUP BY
    slice.weekday,
    browser_family,
    view_count_total
ORDER BY slice.weekday, percent DESC
;
