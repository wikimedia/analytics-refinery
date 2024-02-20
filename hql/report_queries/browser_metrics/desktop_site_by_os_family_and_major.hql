-- Usage
-- spark3-sql -f desktop_site_by_os_family_and_major.hql
--              -d source_table=wmf_traffic.browser_general
--              -d destination_directory=/srv/reportupdater/output/metrics/browser
--              -d start_date=2021-03-12
--              -d end_date=2021-03-19
--              -d coalesce_partitions=1
--

INSERT OVERWRITE DIRECTORY '${destination_directory}'
    USING CSV OPTIONS ('sep' '\t', 'header' 'true', 'compression' 'none')
SELECT
    /*+ COALESCE(${coalesce_partitions}) */
    date_sub(day, (dayofweek(day)-1)) as day,
    os_family,
    os_major,
    SUM(view_count) as view_count
FROM ${source_table}
WHERE
    access_method = 'desktop' AND
    -- Add precise date filtering
    day >= '${start_date}' AND
    day < '${end_date}'
GROUP BY
    date_sub(day, (dayofweek(day)-1)),
    os_family,
    os_major
ORDER BY date_sub(day, (dayofweek(day)-1)), view_count DESC
;