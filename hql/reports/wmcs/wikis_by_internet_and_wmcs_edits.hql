-- Parameters:
--     source_table         -- Fully qualified table name to compute the
--                             aggregation for.
--     destination_dir      -- HDFS directory path where the query results file will
--                             be saved in
--     agg_results_path     -- Path to the resulting aggregate report TSV file
--     start_date           -- Starting date of the computation
--
-- Usage:
-- spark3-sql -f wikis_by_internet_and_wmcs_edits.hql                                                \
--            -d source_table=wmf.editors_daily          \
--            -d destination_dir=/wmf/tmp/analytics/reports/wmcs/wikis_by_internet_and_wmcs_edits       \
--            -d agg_results_path=/wmf/data/published/datasets/periodic/reports/wmcs/wikis_by_internet_and_wmcs_edits.tsv    \
--            -d start_date=2024-02-12                                     \

CREATE TEMPORARY VIEW agg_results_view
USING CSV
OPTIONS (
    'path' '${agg_results_path}',
    'header' 'true',
    'delimiter' '\t',
    'inferSchema' 'true'
)
;

WITH final_daily AS (
    SELECT
        '${start_date}' AS `date`,
        wiki_db,
        SUM(IF(network_origin='wikimedia_labs', edit_count, 0)) AS wmcs_edits,
        SUM(edit_count) AS total_edits,
        ROUND(SUM(IF(network_origin='wikimedia_labs', edit_count, 0)) / SUM(edit_count), 3) AS wmcs_percent
    FROM ${source_table}
    WHERE
        month = substr('${start_date}', 1, 7)
    GROUP BY
        wiki_db
), total_agg AS (
    SELECT
    *
    FROM agg_results_view
    WHERE
        `date` != '${start_date}'

    UNION ALL

    SELECT
        *
    FROM final_daily
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
    `date` ASC,
    wmcs_percent DESC
;
