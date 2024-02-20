-- Usage
-- spark3-sql -f desktop_site_by_browser_family_and_major_percent.hql
--              -d source_table=wmf_traffic.browser_general
--              -d destination_directory=/srv/reportupdater/output/metrics/browser
--              -d start_date=2021-03-12
--              -d end_date=2021-03-19
--              -d coalesce_partitions=1
--
WITH
    slice AS (
        SELECT
            date_sub(day, (dayofweek(day)-1)) as weekday,
            *
        FROM ${source_table}
        WHERE
            access_method = 'desktop' AND
            -- Add precise date filtering
            day >= '${start_date}' AND
            day < '${end_date}'
    ),
    total AS (
        SELECT
            weekday,
            SUM(view_count) as view_count_total
        FROM slice
        GROUP BY
            weekday
    )

INSERT OVERWRITE DIRECTORY '${destination_directory}'
    USING CSV OPTIONS ('sep' '\t', 'header' 'true', 'compression' 'none')
SELECT
    /*+ COALESCE(${coalesce_partitions}) */
    slice.weekday as day,
    browser_family,
    browser_major,
    SUM(view_count) / view_count_total AS percent
FROM slice JOIN total ON slice.weekday=total.weekday
GROUP BY
    slice.weekday,
    browser_family,
    browser_major,
    view_count_total
ORDER BY slice.weekday, percent DESC
;