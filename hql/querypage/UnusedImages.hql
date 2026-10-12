-- Outputs unused images list to a directory.
-- Parameters:
--     destination_directory        -- where to write query output.
--     source_table_file            -- Fully qualified hive table to cooresponding to mediawiki's file table
--     source_table_filerevision    -- Fully qualified hive table to cooresponding to mediawiki's filerevision table
--     source_table_page            -- Fully qualified hive table to cooresponding to mediawiki's page table
--     source_table_categorylinks   -- Fully qualified hive table to cooresponding to mediawiki's categorylinks table
--     source_table_linktarget      -- Fully qualified hive table to cooresponding to mediawiki's linktarget table
--     source_table_imagelinks      -- Fully qualified hive table to cooresponding to mediawiki's imagelinks table
--     wiki                         -- wiki to run the computation on
--     year                         -- year of partition to compute from.
--     month                        -- month of partition to compute from.

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
--     -f UnusedImages.hql \
--     -d destination_directory=/tmp/test-UnusedImages \
--     -d source_table_file=wmf_raw.mediawiki_file \
--     -d source_table_filerevision=wmf_raw.mediawiki_filerevision \
--     -d source_table_page=wmf_raw.mediawiki_page \
--     -d source_table_categorylinks=wmf_raw.mediawiki_categorylinks \
--     -d source_table_linktarget=wmf_raw.mediawiki_private_linktarget \
--     -d source_table_imagelinks=wmf_raw.mediawiki_imagelinks \
--     -d wiki=rowiki \
--     -d year=2023 \
--     -d month=10

SET snapshot = CONCAT(LPAD('${year}', 4, '0'), '-', LPAD('${month}', 2, '0'));

WITH categorized_files AS (
    SELECT
        p.page_title AS file_name
    FROM ${source_table_page} p
    JOIN ${source_table_categorylinks} cl ON (
        cl.cl_from = p.page_id
        AND cl.wiki_db = '${wiki}'
        AND cl.snapshot = ${snapshot}
    )
    WHERE p.snapshot = ${snapshot}
        AND p.wiki_db = '${wiki}'
        AND p.page_namespace = 6  -- NS_FILE
),
linked_files AS (
    SELECT
        lt.lt_title AS file_name
    FROM ${source_table_linktarget} lt
    JOIN ${source_table_imagelinks} il ON (
        il.il_target_id = lt.lt_id
        AND il.wiki_db = '${wiki}'
        AND il.snapshot = ${snapshot}
    )
    WHERE lt.snapshot = ${snapshot}
        AND lt.wiki_db = '${wiki}'
        AND lt.lt_namespace = 6  -- NS_FILE
),
output as (
    SELECT
        'Unusedimages' AS `qc_type`,
        6 AS `qc_namespace`,  -- NS_FILE
        f.file_name AS `qc_title`,
        fr.fr_timestamp AS `qc_value`,
        '${wiki}' as `qc_wiki`,
        ${snapshot} as `qc_snapshot`
    FROM ${source_table_file} f
    JOIN ${source_table_filerevision} fr ON (
        f.file_latest = fr.fr_id
        AND f.wiki_db = '${wiki}'
        AND f.snapshot = ${snapshot}
        AND fr.wiki_db = '${wiki}'
        AND fr.snapshot = ${snapshot}
    )
    LEFT ANTI JOIN categorized_files cf ON f.file_name = cf.file_name
    LEFT ANTI JOIN linked_files lf ON f.file_name = lf.file_name
    WHERE f.file_deleted = 0
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
        )) as unused_images
    FROM output
