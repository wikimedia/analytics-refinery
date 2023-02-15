-- Transform a day of pageview data into a zipped tsv file,
-- each row containing hourly values encoded in Pagecounts-EZ
-- format.  Can only do this for one agent type at a time.
--
-- Parameters:
--     agent_type               -- Filter to only this agent type
--     source_table             -- Fully qualified table name to get pageviews from.
--     destination_directory    -- Where to put the generated file
--     year
--     month
--     day

-- Usage:
-- spark3-sql -f make_hourly_dumps.hql                          \
--      --master yarn --executor-cores 2 --executor-memory 1G   \
--      --conf spark.dynamicAllocation.maxExecutors=32          \
--      --conf spark.executor.memoryOverhead=7G                 \
--      -d agent_type=user                                      \
--      -d source_table=wmf.pageview_hourly                     \
--      -d destination_directory=hdfs:///user/milimetric/test   \
--      -d year=2023                                            \
--      -d month=2                                              \
--      -d day=7

-- Note:
--   We set low executor memory (1G) and high overhead (7G) because the executors mostly
--   read/aggregate and then write, with the exception of the last stage which does a
--   big sort.  So the minimum executor memory lets everything get by without too much
--   allocation, and the overhead enables the sort.

set spark.sql.shuffle.partitions=256;

-- Note: contrary to what spark docs say about the REPARTITION hint, it seems that it
--   ignores any parameters.  In our trials, REPARTITION(128) here had the same effect
--   as plain REPARTITION with the explicit set of shuffle.partitions above.  Without
--   that explicit set, REPARTITION ignores parameters and Spark uses 200 (the default)
with pre_aggregated as (
select /*+ REPARTITION */
    project,
    page_title,
    coalesce(cast(page_id as string), 'null'),
    regexp_replace(access_method, ' ', '-'),
    cast(sum(view_count) as string),
    regexp_replace(concat(
        concat('A', cast(sum(if(hour=0, view_count, 0)) as string)),
        concat('B', cast(sum(if(hour=1, view_count, 0)) as string)),
        concat('C', cast(sum(if(hour=2, view_count, 0)) as string)),
        concat('D', cast(sum(if(hour=3, view_count, 0)) as string)),
        concat('E', cast(sum(if(hour=4, view_count, 0)) as string)),
        concat('F', cast(sum(if(hour=5, view_count, 0)) as string)),
        concat('G', cast(sum(if(hour=6, view_count, 0)) as string)),
        concat('H', cast(sum(if(hour=7, view_count, 0)) as string)),
        concat('I', cast(sum(if(hour=8, view_count, 0)) as string)),
        concat('J', cast(sum(if(hour=9, view_count, 0)) as string)),
        concat('K', cast(sum(if(hour=10, view_count, 0)) as string)),
        concat('L', cast(sum(if(hour=11, view_count, 0)) as string)),
        concat('M', cast(sum(if(hour=12, view_count, 0)) as string)),
        concat('N', cast(sum(if(hour=13, view_count, 0)) as string)),
        concat('O', cast(sum(if(hour=14, view_count, 0)) as string)),
        concat('P', cast(sum(if(hour=15, view_count, 0)) as string)),
        concat('Q', cast(sum(if(hour=16, view_count, 0)) as string)),
        concat('R', cast(sum(if(hour=17, view_count, 0)) as string)),
        concat('S', cast(sum(if(hour=18, view_count, 0)) as string)),
        concat('T', cast(sum(if(hour=19, view_count, 0)) as string)),
        concat('U', cast(sum(if(hour=20, view_count, 0)) as string)),
        concat('V', cast(sum(if(hour=21, view_count, 0)) as string)),
        concat('W', cast(sum(if(hour=22, view_count, 0)) as string)),
        concat('X', cast(sum(if(hour=23, view_count, 0)) as string))
    ), '[A-Z]0', '')
from ${source_table}
where
    year=${year}
    and month=${month}
    and day=${day}
    and agent_type='${agent_type}'
group by project, page_title,
         coalesce(cast(page_id as string), 'null'), regexp_replace(access_method, ' ', '-')
)

insert overwrite directory "${destination_directory}"
using csv
options ('compression' 'bzip2', 'sep' ' ')
-- coalesce 1 to get a single file as the output, for easy archiving
select /*+ coalesce(1) */ *
  from pre_aggregated
 order by project, page_title
;
