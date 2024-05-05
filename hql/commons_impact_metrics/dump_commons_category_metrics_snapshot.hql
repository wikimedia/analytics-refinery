-- Dump Commons Category Metrics Snapshot
--
-- Formats the wmf_contributors.commons_category_metrics_snapshot data
-- as a compressed TSV to be served in dumps.wikimedia.org.
--
-- Parameters:
--
--     source_table            string   Fully qualified name of the source
--                                      commons_category_metrics_snapshot table.
--     destination_directory   string   HDFS path of the directory where to store
--                                      the formatted dump file.
--     year_month              string   Month for which to format the data.
--                                      (YYYY-MM)
--
-- Usage:
--
--     spark3-sql -f dump_commons_category_metrics_snapshot.hql \
--         --master yarn \
--         --executor-cores 2 \
--         --executor-memory 2G \
--         --conf spark.dynamicAllocation.maxExecutors=4 \
--         --conf spark.executor.memoryOverhead=1G \
--         -d source_table=wmf_contributors.commons_category_metrics_snapshot \
--         -d destination_directory=hdfs:///user/mforns/test \
--         -d year_month=2024-02
--

insert overwrite directory "${destination_directory}"
using csv
options ('compression' 'bzip2', 'sep' '	')
-- Coalesce to 1 to ensure there's only 1 output file.
select /*+ coalesce(1) */
    category,
    concat_ws('|', parent_categories),
    concat_ws('|', primary_categories),
    media_file_count,
    media_file_count_deep,
    used_media_file_count,
    used_media_file_count_deep,
    leveraging_wiki_count,
    leveraging_wiki_count_deep,
    leveraging_page_count,
    leveraging_page_count_deep,
    year_month
from ${source_table}
where year_month = '${year_month}'
;
