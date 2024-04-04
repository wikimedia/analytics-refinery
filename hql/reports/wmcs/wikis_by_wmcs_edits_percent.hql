-- Parameters:
--     source_table         -- Fully qualified table name to compute the
--                             aggregation for.
--     destination_dir      -- HDFS directory path where the query results file will
--                             be saved in
--     start_date           -- Starting date of the computation
--
-- Usage:
-- spark3-sql -f wikis_by_wmcs_edits_percent.hql                                        \
--            -d source_table=wmf.editors_daily     \
--            -d destination_dir=/wmf/tmp/analytics/reports/wmcs/wikis_by_wmcs_edits_percent
--            -d start_date=2024-02-12                                \


WITH slice AS (
    SELECT
        wiki_db,
        SUM(IF(network_origin='wikimedia_labs', edit_count, 0)) AS wmcs_edits,
        SUM(edit_count) AS total_edits,
        ROUND(SUM(IF(network_origin='wikimedia_labs', edit_count, 0)) / SUM(edit_count), 3) AS wmcs_percent
    FROM ${source_table}
    WHERE
        month = substr('${start_date}', 1, 7)
    GROUP BY
        wiki_db
),
results AS (
    SELECT
        '${start_date}' AS `date`,
        'TOTAL' AS wiki_db,
        ROUND(SUM(wmcs_edits) / SUM(total_edits), 3) AS wmcs_percent
    FROM slice

    UNION ALL

    SELECT
        '${start_date}' AS `date`,
        wiki_db,
        wmcs_percent
    FROM slice
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
FROM results
ORDER BY
    `date` ASC,
    wiki_db ASC
;
