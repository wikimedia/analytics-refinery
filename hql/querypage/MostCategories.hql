-- Outputs most linked templates list to a directory.
-- Parameters:
--     destination_directory      -- where to write query output.
--     source_table_categorylinks -- Fully qualified hive table to cooresponding to mediawiki's categorylinks table.
--     source_table_page          -- Fully qualified hive table to cooresponding to mediawiki's page table
--     wiki                       -- wiki to run the computation on
--     year                       -- year of partition to compute from.
--     month                      -- month of partition to compute from.

-- Usage:
-- spark3-sql \
-- --driver-cores 1 \
-- --driver-memory 4G \
-- --executor-cores 1 \
-- --executor-memory 1G \
-- --master yarn \
-- --conf spark.hadoop.mapred.output.compress=false \
-- --conf spark.dynamicAllocation.maxExecutors=16 \
-- --conf spark.yarn.maxAppAttempts=1 \
-- --conf spark.executor.memoryOverhead=2G \
--     -f MostTranscludedPages.hql \
--     -d destination_directory=/tmp/test-MostTranscludedPages \
--     -d source_table_categorylinks=wmf_raw.mediawiki_categorylinks  \
--     -d source_table_page=wmf_raw.mediawiki_page  \
--     -d wiki=rowiki \
--     -d year=2023 \
--     -d month=10

SET snapshot = CONCAT(LPAD('${year}', 4, '0'), '-', LPAD('${month}', 2, '0'));

WITH output as (
    SELECT
        'Mostcategories' AS `qc_type`,
        page_namespace AS `qc_namespace`,
        page_title AS `qc_title`,
        COUNT(1) AS `qc_value`,
        '${wiki}' as `qc_wiki`,
        ${snapshot} as `qc_snapshot`
    FROM ${source_table_categorylinks}
    LEFT JOIN ${source_table_page} ON ((cl_from=page_id))
    WHERE ${source_table_categorylinks}.snapshot = ${snapshot}
        AND ${source_table_categorylinks}.wiki_db = '${wiki}'
        AND ${source_table_page}.snapshot = ${snapshot}
        AND ${source_table_page}.wiki_db = '${wiki}'
        AND ${source_table_page}.page_namespace = 0
    GROUP BY qc_namespace, qc_title
    HAVING qc_value > 1
    ORDER BY qc_value DESC
    LIMIT 5000
)

-- will output a file that looks like [{"qc_type":"...},...,{...}]
INSERT OVERWRITE DIRECTORY "${destination_directory}"
USING text
    SELECT /*+ COALESCE(1) */
        to_json(collect_list(
            named_struct(
                'qc_type', qc_type,
                'qc_namespace', qc_namespace,
                'qc_title', qc_title,
                'qc_value', qc_value,
                'qc_wiki', qc_wiki,
                'qc_snapshot', qc_snapshot
            )
        )) as most_categories
    FROM output
