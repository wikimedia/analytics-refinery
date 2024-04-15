-- Dump Commons Media File Metrics Snapshot
--
-- Formats the wmf_contributors.commons_media_file_metrics_snapshot data
-- as a compressed TSV to be served in dumps.wikimedia.org.
--
-- Parameters:
--
--     source_table            string   Fully qualified name of the source
--                                      commons_media_file_metrics_snapshot table.
--     destination_directory   string   HDFS path of the directory where to store
--                                      the formatted dump file.
--     snapshot                string   Month for which to format the data.
--                                      (YYYY-MM)
--
-- Usage:
--
--     spark3-sql -f dump_commons_media_file_metrics_snapshot.hql \
--         --master yarn \
--         --executor-cores 4 \
--         --executor-memory 8G \
--         --conf spark.dynamicAllocation.maxExecutors=8 \
--         --conf spark.executor.memoryOverhead=1G \
--         -d source_table=wmf_contributors.commons_media_file_metrics_snapshot \
--         -d destination_directory=hdfs:///user/mforns/test \
--         -d snapshot=2024-02
--

insert overwrite directory "${destination_directory}"
using csv
options ('compression' 'bzip2', 'sep' '	')
-- Coalesce to 1 to ensure there's only 1 output file.
select /*+ coalesce(1) */
    media_file,
    media_type,
    concat_ws('|', categories),
    concat_ws('|', primary_categories),
    leveraging_wiki_count,
    leveraging_page_count,
    month
from ${source_table}
where month = '${snapshot}'
;
