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
-- spark3-sql -f machine.hql                                       \
--            -d source_table=event_sanitized.mediawikipingback    \
--            -d destination_dir=/wmf/tmp/analytics/reports/pingback/machine
--            -d agg_results_path=/wmf/data/published/datasets/periodic/reports/pingback/machine.tsv    \
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
        event.machine AS event_machine,
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
        event.machine != ''
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
        SUM(IF(event_machine = 'x86_64', 1, 0)) AS `x86_64`,
        SUM(IF(event_machine = 'i386', 1, 0)) AS `i386`,
        SUM(IF(event_machine = 'i586', 1, 0)) AS `i586`,
        SUM(IF(event_machine = 'i686', 1, 0)) AS `i686`,
        SUM(IF(event_machine = 'amd64' OR event_machine ='AMD64', 1, 0)) AS `amd64`,
        SUM(IF(event_machine LIKE 'arm%', 1, 0)) AS `arm`,
        SUM(IF(event_machine LIKE 'ppc%', 1, 0)) AS `ppc`,
        SUM(IF(event_machine != 'x86_64' AND
            event_machine != 'i386' AND
            event_machine != 'i586' AND
            event_machine != 'i686' AND
            event_machine != 'amd64' AND event_machine !='AMD64' AND
            event_machine NOT LIKE 'arm%' AND
            event_machine NOT LIKE 'ppc%', 1, 0)) AS `other`
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
