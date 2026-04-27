-- Outputs wanted categories list to a directory.
-- Parameters:
--     destination_directory      -- where to write query output.
--     source_table_categorylinks -- Fully qualified hive table to cooresponding to mediawiki's categorylinks table
--     source_table_linktarget    -- Fully qualified hive table to cooresponding to mediawiki's linktarget table
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
--     -f WantedCategories.hql \
--     -d destination_directory=/tmp/test-WantedCategories \
--     -d source_table_categorylinks=wmf_raw.mediawiki_categorylinks  \
--     -d source_table_linktarget=wmf_raw.mediawiki_linktarget  \
--     -d source_table_page=wmf_raw.mediawiki_page  \
--     -d wiki=rowiki \
--     -d year=2023 \
--     -d month=10

SET snapshot = CONCAT(LPAD('${year}', 4, '0'), '-', LPAD('${month}', 2, '0'));

WITH output as (
    SELECT
        'Wantedcategories' AS `qc_type`,
        14 AS `qc_namespace`,  -- NS_CATEGORY
        lt.lt_title AS `qc_title`,
        COUNT(*) AS `qc_value`,
        '${wiki}' as `qc_wiki`,
        ${snapshot} as `qc_snapshot`
    FROM ${source_table_categorylinks} cl
    INNER JOIN ${source_table_linktarget} lt ON (
        cl.cl_target_id = lt.lt_id
        AND cl.wiki_db = '${wiki}'
        AND cl.snapshot = ${snapshot}
        AND lt.wiki_db = '${wiki}'
        AND lt.snapshot = ${snapshot}
    )
    LEFT JOIN ${source_table_page} p ON (
        p.page_title = lt.lt_title
        AND p.page_namespace = 14  -- NS_CATEGORY
        AND p.wiki_db = '${wiki}'
        AND p.snapshot = ${snapshot}
    )
    WHERE cl.wiki_db = '${wiki}'
        AND cl.snapshot = ${snapshot}
        AND p.page_title IS NULL  -- Category page doesn't exist
    GROUP BY lt.lt_title
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
        )) as wanted_categories
    FROM output
