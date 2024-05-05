-- Dump Commons Pageviews per Category Monthly
--
-- Formats the wmf_contributors.commons_pageviews_per_category_monthly data
-- as a compressed TSV to be served in dumps.wikimedia.org.
--
-- Parameters:
--
--     source_table            string   Fully qualified name of the source
--                                      commons_pageviews_per_category_monthly table.
--     destination_directory   string   HDFS path of the directory where to store
--                                      the formatted dump file.
--     year_month              string   Month for which to format the data.
--                                      (YYYY-MM)
--
-- Usage:
--
--     spark3-sql -f dump_commons_pageviews_per_category_monthly.hql \
--         --master yarn \
--         --executor-cores 4 \
--         --executor-memory 8G \
--         --conf spark.dynamicAllocation.maxExecutors=12 \
--         --conf spark.executor.memoryOverhead=2G \
--         -d source_table=wmf_contributors.commons_pageviews_per_category_monthly \
--         -d destination_directory=hdfs:///user/mforns/test \
--         -d year_month=2024-02
--

insert overwrite directory "${destination_directory}"
using csv
options ('compression' 'bzip2', 'sep' '	')
-- Coalesce to 1 to ensure there's only 1 output file.
select /*+ coalesce(1) */
    category,
    category_scope,
    concat_ws('|', primary_categories),
    wiki,
    page_title,
    pageview_count,
    year_month
from ${source_table}
where year_month = '${year_month}'
;
