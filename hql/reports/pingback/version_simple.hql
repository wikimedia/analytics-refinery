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
-- spark3-sql -f version_simple.hql                                \
--            -d source_table=event_sanitized.mediawikipingback    \
--            -d destination_dir=/wmf/tmp/analytics/reports/pingback/version_simple
--            -d agg_results_path=/wmf/data/published/datasets/periodic/reports/pingback/version_simple.tsv    \
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
        event.MediaWiki != ''
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
        SUM(IF(event_mediawiki LIKE '1.28%', 1, 0)) AS `1.28`,
        SUM(IF(event_mediawiki LIKE '1.29%', 1, 0)) AS `1.29`,
        SUM(IF(event_mediawiki LIKE '1.30%', 1, 0)) AS `1.30`,
        SUM(IF(event_mediawiki LIKE '1.31%', 1, 0)) AS `1.31`,
        SUM(IF(event_mediawiki LIKE '1.32%', 1, 0)) AS `1.32`,
        SUM(IF(event_mediawiki LIKE '1.33%', 1, 0)) AS `1.33`,
        SUM(IF(event_mediawiki LIKE '1.34%', 1, 0)) AS `1.34`,
        SUM(IF(event_mediawiki LIKE '1.35%', 1, 0)) AS `1.35`,
        SUM(IF(event_mediawiki LIKE '1.36%', 1, 0)) AS `1.36`,
        SUM(IF(event_mediawiki LIKE '1.37%', 1, 0)) AS `1.37`,
        SUM(IF(event_mediawiki LIKE '1.38%', 1, 0)) AS `1.38`,
        SUM(IF(event_mediawiki LIKE '1.39%', 1, 0)) AS `1.39`,
        SUM(IF(event_mediawiki LIKE '1.40%', 1, 0)) AS `1.40`,
        SUM(IF(event_mediawiki LIKE '1.41%', 1, 0)) AS `1.41`,
        SUM(IF(event_mediawiki LIKE '1.42%', 1, 0)) AS `1.42`,
        SUM(IF(event_mediawiki NOT LIKE '1.28%' AND
            event_mediawiki NOT LIKE '1.29%' AND
            event_mediawiki NOT LIKE '1.30%' AND
            event_mediawiki NOT LIKE '1.31%' AND
            event_mediawiki NOT LIKE '1.32%' AND
            event_mediawiki NOT LIKE '1.33%' AND
            event_mediawiki NOT LIKE '1.34%' AND
            event_mediawiki NOT LIKE '1.35%' AND
            event_mediawiki NOT LIKE '1.36%' AND
            event_mediawiki NOT LIKE '1.37%' AND
            event_mediawiki NOT LIKE '1.38%' AND
            event_mediawiki NOT LIKE '1.39%' AND
            event_mediawiki NOT LIKE '1.40%' AND
            event_mediawiki NOT LIKE '1.41%' AND
            event_mediawiki NOT LIKE '1.42%', 1, 0)) AS `other`
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
