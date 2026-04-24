-- Outputs unused templates list to a directory.
-- Parameters:
--     destination_directory      -- where to write query output.
--     source_table_page          -- Fully qualified hive table to cooresponding to mediawiki's page table
--     source_table_linktarget    -- Fully qualified hive table to cooresponding to mediawiki's linktarget table
--     source_table_templatelinks -- Fully qualified hive table to cooresponding to mediawiki's templatelinks table
--     source_table_page_props    -- Fully qualified hive table to cooresponding to mediawiki's page_props table
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
--     -f UnusedTemplates.hql \
--     -d destination_directory=/tmp/test-UnusedTemplates \
--     -d source_table_page=wmf_raw.mediawiki_page  \
--     -d source_table_linktarget=wmf_raw.mediawiki_linktarget  \
--     -d source_table_templatelinks=wmf_raw.mediawiki_templatelinks  \
--     -d source_table_page_props=wmf_raw.mediawiki_page_props  \
--     -d wiki=rowiki \
--     -d year=2023 \
--     -d month=10

SET snapshot = CONCAT(LPAD('${year}', 4, '0'), '-', LPAD('${month}', 2, '0'));

WITH output as (
    SELECT
        'Unusedtemplates' AS `qc_type`,
        p.page_namespace AS `qc_namespace`,
        p.page_title AS `qc_title`,
        1 AS `qc_value`,
        '${wiki}' as `qc_wiki`,
        ${snapshot} as `qc_snapshot`
    FROM ${source_table_page} p
    LEFT JOIN ${source_table_linktarget} lt ON (
        lt.lt_title = p.page_title
        AND lt.lt_namespace = p.page_namespace
        AND lt.wiki_db = '${wiki}'
        AND lt.snapshot = ${snapshot}
    )
    LEFT JOIN ${source_table_templatelinks} tl ON (
        tl.tl_target_id = lt.lt_id
        AND tl.wiki_db = '${wiki}'
        AND tl.snapshot = ${snapshot}
    )
    LEFT JOIN ${source_table_page_props} pp ON (
        p.page_id = pp.pp_page
        AND pp.pp_propname = 'expectunusedtemplate'
        AND pp.wiki_db = '${wiki}'
        AND pp.snapshot = ${snapshot}
    )
    WHERE p.snapshot = ${snapshot}
        AND p.wiki_db = '${wiki}'
        AND p.page_namespace = 10  -- NS_TEMPLATE
        AND p.page_is_redirect = 0
        AND tl.tl_from IS NULL  -- No templatelinks entries
        AND pp.pp_page IS NULL  -- No expectunusedtemplate property
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
        )) as unused_templates
    FROM output
