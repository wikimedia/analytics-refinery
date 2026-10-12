-- Outputs disambiguation page links list to a directory.
-- Parameters:
--     destination_directory        -- where to write query output.
--     source_table_linktarget     -- Fully qualified hive table to cooresponding to mediawiki's linktarget table
--     source_table_pagelinks      -- Fully qualified hive table to cooresponding to mediawiki's pagelinks table
--     source_table_page           -- Fully qualified hive table to cooresponding to mediawiki's page table
--     source_table_page_props     -- Fully qualified hive table to cooresponding to mediawiki's page_props table
--     namespace_map_table         -- Fully qualified hive table for mediawiki_project_namespace_map
--     wiki                        -- wiki to run the computation on
--     year                        -- year of partition to compute from.
--     month                       -- month of partition to compute from.

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
--     -f DisambiguationPageLinks.hql \
--     -d destination_directory=/tmp/test-DisambiguationPageLinks \
--     -d source_table_linktarget=wmf_raw.mediawiki_private_linktarget \
--     -d source_table_pagelinks=wmf_raw.mediawiki_pagelinks \
--     -d source_table_page=wmf_raw.mediawiki_page \
--     -d source_table_page_props=wmf_raw.mediawiki_page_props \
--     -d namespace_map_table=wmf_raw.mediawiki_project_namespace_map \
--     -d wiki=rowiki \
--     -d year=2023 \
--     -d month=10

SET snapshot = CONCAT(LPAD('${year}', 4, '0'), '-', LPAD('${month}', 2, '0'));

WITH output as (
    SELECT
        'Disambiguationpagelinks' AS `qc_type`,
        p2.page_namespace AS `qc_namespace`,
        p2.page_title AS `qc_title`,
        pl.pl_from AS `qc_value`,
        p1.page_namespace AS `qc_to_namespace`,
        p1.page_title AS `qc_to_title`,
        '${wiki}' as `qc_wiki`,
        ${snapshot} as `qc_snapshot`
    FROM ${source_table_linktarget} lt
    JOIN ${source_table_pagelinks} pl ON (
        pl.pl_target_id = lt.lt_id
        AND pl.wiki_db = '${wiki}'
        AND pl.snapshot = ${snapshot}
    )
    JOIN ${source_table_page} p1 ON (
        lt.lt_namespace = p1.page_namespace
        AND lt.lt_title = p1.page_title
        AND p1.wiki_db = '${wiki}'
        AND p1.snapshot = ${snapshot}
    )
    JOIN ${source_table_page_props} pp ON (
        p1.page_id = pp.pp_page
        AND pp.pp_propname = 'disambiguation'
        AND pp.wiki_db = '${wiki}'
        AND pp.snapshot = ${snapshot}
    )
    JOIN ${source_table_page} p2 ON (
        p2.page_id = pl.pl_from
        AND p2.wiki_db = '${wiki}'
        AND p2.snapshot = ${snapshot}
        AND p2.page_is_redirect = 0
    )
    JOIN ${namespace_map_table} nsm ON (
        p2.wiki_db = nsm.dbname
        AND p2.page_namespace = nsm.namespace
        AND nsm.snapshot = ${snapshot}
        AND nsm.namespace_is_content = 1
    )
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
                'qc_to_namespace', qc_to_namespace,
                'qc_to_title', qc_to_title,
                'qc_wiki', qc_wiki,
                'qc_snapshot', qc_snapshot
            )
        )) as disambiguation_page_links
    FROM output
