-- Outputs most linked templates list to a directory.
-- Parameters:
--     destination_directory      -- where to write query output.
--     source_table_templatelinks -- Fully qualified hive table to cooresponding to mediawiki's templatelinks table.
--     source_table_linktarget    -- Fully qualified hive table to cooresponding to mediawiki's linktarget table
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
--     -d source_table_templatelinks=wmf_raw.mediawiki_templatelinks  \
--     -d source_table_linktarget=wmf_raw.mediawiki_private_linktarget  \
--     -d wiki=rowiki \
--     -d year=2023 \
--     -d month=10

WITH output as (
    SELECT
        'MostTranscludedPages' AS `qc_type`,
        lt_namespace AS `qc_namespace`,
        lt_title AS `qc_title`,
        COUNT(1) AS `qc_value`,
        '${wiki}' as `qc_wiki`,
        '${year}-${month}' as `qc_snapshot`
    FROM ${source_table_templatelinks}
    JOIN ${source_table_linktarget} ON ((tl_target_id=lt_id))
    WHERE ${source_table_templatelinks}.snapshot = '${year}-${month}'
        AND ${source_table_templatelinks}.wiki_db = '${wiki}'
        AND ${source_table_linktarget}.snapshot = '${year}-${month}'
        AND ${source_table_linktarget}.wiki_db = '${wiki}'
    GROUP BY qc_namespace, qc_title
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
        )) as most_transcluded_pages
    FROM output
