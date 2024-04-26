-- Parameters:
--     source_table         -- Fully qualified table name to compute the
--                             aggregation for.
--     destination_dir      -- HDFS directory path where the query results file will
--                             be saved in
--     agg_results_path     -- Path to the resulting aggregate report TSV file
--     explode_by           -- Value of field event.MediaWiki to "explode" this query by.
--                             Generate a separate query for each version of field event.MediaWiki.
--                             Example values: 1.28, 1.29, 1.30, ...
--     coalesce_partitions  -- Number of partitions to write
--     start_date           -- Starting date of the computation
--     end_date             -- End date of the computation (start_date + 7 days)
--
-- Usage:
-- spark3-sql -f php_drilldown.hql                                            \
--            -d source_table=event_sanitized.mediawikipingback               \
--            -d destination_dir=/wmf/tmp/analytics/reports/pingback/php_drilldown   \
--            -d agg_results_path=/wmf/data/published/datasets/periodic/reports/pingback/php_drilldown/1.35.tsv    \
--            -d explode_by=1.35                                              \
--            -d start_date=20240212                                          \
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
        event.PHP AS event_php,
        event.MediaWiki AS event_mediawiki,
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
        event.PHP != ''
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
        SUM(IF(event_php LIKE '5.5%', 1, 0)) AS `5.5`,
        SUM(IF(event_php LIKE '5.6%', 1, 0)) AS `5.6`,
        SUM(IF(event_php LIKE '7.0%', 1, 0)) AS `7.0`,
        SUM(IF(event_php LIKE '7.1%', 1, 0)) AS `7.1`,
        SUM(IF(event_php LIKE '7.2%', 1, 0)) AS `7.2`,
        SUM(IF(event_php LIKE '7.3%', 1, 0)) AS `7.3`,
        SUM(IF(event_php LIKE '7.4%', 1, 0)) AS `7.4`,
        SUM(IF(event_php LIKE '8.0%', 1, 0)) AS `8.0`,
        SUM(IF(event_php LIKE '8.1%', 1, 0)) AS `8.1`,
        SUM(IF(event_php LIKE '8.2%', 1, 0)) AS `8.2`,
        SUM(IF(event_php LIKE '8.3%', 1, 0)) AS `8.3`,
        SUM(IF(event_php LIKE '8.4%', 1, 0)) AS `8.4`,
        SUM(IF(event_php NOT LIKE '5.5%' AND
        event_php NOT LIKE '5.6%' AND
        event_php NOT LIKE '7.0%' AND
        event_php NOT LIKE '7.1%' AND
        event_php NOT LIKE '7.2%' AND
        event_php NOT LIKE '7.3%' AND
        event_php NOT LIKE '7.4%' AND
        event_php NOT LIKE '8.0%' AND
        event_php NOT LIKE '8.1%' AND
        event_php NOT LIKE '8.2%' AND
        event_php NOT LIKE '8.3%' AND
        event_php NOT LIKE '8.4%', 1, 0)) AS `other`
    FROM numbered
    WHERE
        row_num = 1 AND
        event_mediawiki LIKE '${explode_by}%'
)
INSERT OVERWRITE DIRECTORY '${destination_dir}'
USING CSV
OPTIONS (
    'sep' '\t',
    'header' 'true',
    'compression' 'none',
    'emptyValue' ''
)
SELECT /*+ COALESCE(1) */
    *
FROM total_agg
ORDER BY
    `date` ASC
;
