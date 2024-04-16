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
-- spark3-sql -f version.hql                                        \
--            -d source_table=event_sanitized.mediawikipingback     \
--            -d destination_dir=/wmf/tmp/analytics/reports/pingback/version
--            -d agg_results_path=/wmf/data/published/datasets/periodic/reports/pingback/version.tsv    \
--            -d start_date=20240212                                \
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
results AS (
    SELECT
        CASE
            WHEN event_mediawiki RLIKE '^[0-9]+.[0-9]+.[0-9]+$'
                THEN event_mediawiki
            WHEN event_mediawiki RLIKE '^[0-9]+.[0-9]+.[0-9]+-wmf'
                THEN CONCAT(SUBSTR(event_mediawiki, 0, LOCATE('-wmf', event_mediawiki) - 1), ' (WMF)')
            WHEN event_mediawiki RLIKE '^[0-9]+.[0-9]+.[0-9]+-rc'
                THEN CONCAT(SUBSTR(event_mediawiki, 0, LOCATE('-rc', event_mediawiki) - 1), ' (RC)')
            WHEN event_mediawiki RLIKE '^[0-9]+.[0-9]+.[0-9]+-alpha'
                THEN CONCAT(SUBSTR(event_mediawiki, 0, LOCATE('-alpha', event_mediawiki) - 1), ' (ALPHA)')
            ELSE SUBSTR(event_mediawiki, 0, 6)
        END AS version
    FROM numbered
    WHERE row_num = 1
),
final_daily AS (
    SELECT
        '${start_date}' AS `date`,
        version,
        COUNT(*) as count
    FROM results
    GROUP BY
        version
    ORDER BY count DESC
),
total_agg AS (
    SELECT
        *
    FROM final_daily

    UNION ALL

    SELECT
        *
    FROM agg_results_view
    WHERE
        `date` != '${start_date}'
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
