-- Parameters:
--     source_table         -- Fully qualified table name to compute the
--                             aggregation for.
--     destination_dir      -- HDFS directory path where the query results file will
--                             be saved in
--     agg_results_path     -- Path to the resulting aggregate report TSV file
--     start_date           -- Starting date of the computation
--     end_date             -- End date of the computation (start_date + 7 days)
--
-- Usage:
-- spark3-sql -f os.hql                                            \
--            -d source_table=event_sanitized.mediawikipingback    \
--            -d destination_dir=/wmf/tmp/analytics/reports/pingback/os
--            -d agg_results_path=/wmf/data/published/datasets/periodic/reports/pingback/os.tsv    \
--            -d start_date=20240212                               \
--            -d end_date=20240219

CREATE TEMPORARY VIEW agg_results_view
USING CSV
OPTIONS (
    'path' '${agg_results_path}',
    'header' 'true',
    'delimiter' '\t',
    'inferSchema' 'true'
)
;

WITH numbered AS (
    SELECT
        event.OS AS event_os,
        ROW_NUMBER() OVER (
            PARTITION BY wiki
            ORDER BY dt DESC
        ) AS row_num
    FROM
        ${source_table}
    WHERE
        CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) >= date_sub('${start_date}', 23) AND
        CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) < '${end_date}' AND
        (event.`database` LIKE 'mysql%' OR NOT (
            event.MediaWiki LIKE '1.31.0%' OR
            event.MediaWiki = '1.32.0-alpha'
        )) AND
        event.OS != ''
),
total_agg AS (
    SELECT
        *
    FROM agg_results_view
    WHERE
        `date` != '${start_date}'

    UNION ALL

    SELECT
        '${start_date}' AS `date`,
        SUM(IF(event_os LIKE 'Linux%', 1, 0)) AS `linux`,
        SUM(IF(event_os LIKE 'WINNT%', 1, 0)) AS `winnt`,
        SUM(IF(event_os LIKE 'Darwin%', 1, 0)) AS `darwin`,
        SUM(IF(event_os LIKE 'FreeBSD%', 1, 0)) AS `freebsd`,
        SUM(IF(event_os LIKE 'SunOS%', 1, 0)) AS `sunos`,
        SUM(IF(event_os LIKE 'CYGWIN%', 1, 0)) AS `cygwin`,
        SUM(IF(event_os LIKE 'NetBSD%', 1, 0)) AS `netbsd`,
        SUM(IF(event_os LIKE 'OpenBSD%', 1, 0)) AS `openbsd`,
        SUM(IF(event_os NOT LIKE 'Linux%' AND
            event_os NOT LIKE 'WINNT%' AND
            event_os NOT LIKE 'Darwin%' AND
            event_os NOT LIKE 'FreeBSD%' AND
            event_os NOT LIKE 'SunOS%' AND
            event_os NOT LIKE 'CYGWIN%' AND
            event_os NOT LIKE 'NetBSD%' AND
            event_os NOT LIKE 'OpenBSD%', 1, 0)) AS `other`
    FROM numbered
    WHERE row_num = 1
)
INSERT OVERWRITE DIRECTORY '${destination_dir}'
USING CSV
OPTIONS (
    'sep' '\t',
    'header' 'true',
    'compression' 'none'
)
SELECT /*+ COALESCE(1) */
    *
FROM total_agg
ORDER BY
    `date` ASC
;
