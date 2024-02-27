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
-- spark3-sql -f memory_simple.hql                                 \
--            -d source_table=event_sanitized.mediawikipingback    \
--            -d destination_dir=/wmf/tmp/analytics/reports/pingback/memory_simple
--            -d agg_results_path=/wmf/data/published/datasets/periodic/reports/pingback/memory_simple.tsv    \
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
        CASE
            WHEN event.memoryLimit LIKE '%M' THEN
                CASE
                    WHEN SUBSTR(event.memoryLimit, 0, LENGTH(event.memoryLimit) - 1) RLIKE '^[0-9]+$' THEN
                        CAST(SUBSTR(event.memoryLimit, 0, LENGTH(event.memoryLimit) - 1) AS BIGINT)
                    WHEN SUBSTR(event.memoryLimit, 0, LENGTH(event.memoryLimit) - 1) RLIKE '^[0-9]+([.][0-9]+)*$' THEN
                        CAST(SUBSTR(event.memoryLimit, 0, LOCATE('.', event.memoryLimit) - 1) AS BIGINT)
                    ELSE -1
                END
            WHEN event.memoryLimit LIKE '%G' THEN
                CASE
                    WHEN SUBSTR(event.memoryLimit, 0, LENGTH(event.memoryLimit) - 1) RLIKE '^[0-9]+$' THEN
                        CAST(SUBSTR(event.memoryLimit, 0, LENGTH(event.memoryLimit) - 1) AS BIGINT) / 1024
                    WHEN SUBSTR(event.memoryLimit, 0, LENGTH(event.memoryLimit) - 1) RLIKE '^[0-9]+([.][0-9]+)*$' THEN
                        CAST(SUBSTR(event.memoryLimit, 0, LOCATE('.', event.memoryLimit) - 1) AS BIGINT) / 1024
                    ELSE -1
                END
            WHEN event.memoryLimit RLIKE '^[0-9]+$' THEN
                CAST(event.memoryLimit AS BIGINT) / (1024 * 1024)
            WHEN event.memoryLimit RLIKE '^[0-9]+([.][0-9]+)*$' THEN
                CAST(SUBSTR(event.memoryLimit, 0, LOCATE('.', event.memoryLimit) - 1) AS BIGINT) / (1024 * 1024)
            ELSE -1
        END AS normalizedMemoryLimit,
        ROW_NUMBER() OVER (
            PARTITION BY wiki
            ORDER BY dt DESC
        ) AS row_num
    FROM ${source_table}
    WHERE
        CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) >= date_sub('${start_date}', 23) AND
        CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) < '${end_date}' AND
        (event.`database` LIKE 'mysql%' OR NOT (
            event.MediaWiki LIKE '1.31.0%' OR
            event.MediaWiki = '1.32.0-alpha'
        )) AND
        event.memoryLimit != ''
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
        SUM(IF(normalizedMemoryLimit > 0 AND normalizedMemoryLimit <= 1, 1, 0)) AS `limit <= 1m`,
        SUM(IF(normalizedMemoryLimit > 1 AND normalizedMemoryLimit <= 32, 1, 0)) AS `1m < limit <= 32m`,
        SUM(IF(normalizedMemoryLimit > 32 AND normalizedMemoryLimit <= 64, 1, 0)) AS `32m < limit <= 64m`,
        SUM(IF(normalizedMemoryLimit > 64 AND normalizedMemoryLimit <= 128, 1, 0)) AS `64m < limit <= 128m`,
        SUM(IF(normalizedMemoryLimit > 128 AND normalizedMemoryLimit <= 256, 1, 0)) AS `128m < limit <= 256m`,
        SUM(IF(normalizedMemoryLimit > 256 AND normalizedMemoryLimit <= 512, 1, 0)) AS `256m < limit <= 512m`,
        SUM(IF(normalizedMemoryLimit > 512 AND normalizedMemoryLimit <= 1024, 1, 0)) AS `512m < limit <= 1g`,
        SUM(IF(normalizedMemoryLimit > 1024, 1, 0)) AS `limit > 1g`,
        SUM(IF(normalizedMemoryLimit < 0, 1, 0)) AS `non-numeric`
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
