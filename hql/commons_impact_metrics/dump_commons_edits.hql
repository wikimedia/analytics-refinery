-- Dump Commons Edits
--
-- Formats the wmf_contributors.commons_edits data
-- as a compressed TSV to be served in dumps.wikimedia.org.
--
-- Parameters:
--
--     source_table            string   Fully qualified name of the source
--                                      commons_edits table.
--     destination_directory   string   HDFS path of the directory where to store
--                                      the formatted dump file.
--     year_month              string   Month for which to format the data.
--                                      (YYYY-MM)
--
-- Usage:
--
--     spark3-sql -f dump_commons_edits.hql \
--         --master yarn \
--         --executor-cores 2 \
--         --executor-memory 4G \
--         --conf spark.dynamicAllocation.maxExecutors=4 \
--         --conf spark.executor.memoryOverhead=1G \
--         -d source_table=wmf_contributors.commons_edits \
--         -d destination_directory=hdfs:///user/mforns/test \
--         -d year_month=2024-02
--

insert overwrite directory "${destination_directory}"
using csv
options ('compression' 'bzip2', 'sep' '	')
-- Coalesce to 1 to ensure there's only 1 output file.
select /*+ coalesce(1) */
    user_name,
    edit_type,
    media_file,
    concat_ws('|', categories),
    concat_ws('|', primary_categories),
    date_format(dt, "yyyy-MM-dd'T'HH:mm:ss")
from ${source_table}
where
    dt >= to_timestamp('${year_month}', 'yyyy-MM') and
    dt < add_months(to_timestamp('${year_month}', 'yyyy-MM'), 1)
;
