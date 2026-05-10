-- Outputs uncategorized images list to a directory.
-- Parameters:
--     destination_directory      -- where to write query output.
--     source_table_page          -- Fully qualified hive table to cooresponding to mediawiki's page table
--     source_table_categorylinks -- Fully qualified hive table to cooresponding to mediawiki's categorylinks table
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
--     -f UncategorizedImages.hql \
--     -d destination_directory=/tmp/test-UncategorizedImages \
--     -d source_table_page=wmf_raw.mediawiki_page  \
--     -d source_table_categorylinks=wmf_raw.mediawiki_categorylinks  \
--     -d wiki=commonswiki \
--     -d year=2023 \
--     -d month=10

SET snapshot = CONCAT(LPAD('${year}', 4, '0'), '-', LPAD('${month}', 2, '0'));

WITH output as (
    SELECT
        'Uncategorizedimages' AS `qc_type`,
        p.page_namespace AS `qc_namespace`,
        p.page_title AS `qc_title`,
        1 AS `qc_value`,
        '${wiki}' as `qc_wiki`,
        ${snapshot} as `qc_snapshot`
    FROM ${source_table_page} p
    LEFT JOIN ${source_table_categorylinks} cl ON (
        cl.cl_from = p.page_id
        AND cl.wiki_db = '${wiki}'
        AND cl.snapshot = ${snapshot}
    )
    WHERE p.snapshot = ${snapshot}
        AND p.wiki_db = '${wiki}'
        AND p.page_namespace = 6  -- NS_FILE
        AND p.page_is_redirect = 0
        AND cl.cl_from IS NULL  -- No categorylinks entries
    ORDER BY qc_title ASC
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
        )) as uncategorized_images
    FROM output
