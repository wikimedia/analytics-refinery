-- Transform a month of pageview data into a zipped tsv file,
-- each row containing daily values encoded in Pagecounts-EZ
-- format.  Can only do this for one agent type at a time.
--
-- Parameters:
--     agent_type               -- Filter to only this agent type
--     source_table             -- Fully qualified table name to get pageviews from.
--     destination_directory    -- Where to put the generated file
--     year
--     month

-- Usage:
-- spark3-sql -f make_daily_dumps.hql                           \
--      --master yarn --executor-cores 2 --executor-memory 1G   \
--      --conf spark.dynamicAllocation.maxExecutors=64          \
--      --conf spark.executor.memoryOverhead=7G                 \
--      -d agent_type=user                                      \
--      -d source_table=wmf.pageview_hourly                     \
--      -d destination_directory=hdfs:///user/milimetric/test_m \
--      -d year=2023                                            \
--      -d month=1

-- Note:
--   We set low executor memory (1G) and high overhead (7G) because the executors mostly
--   read/aggregate and then write, with the exception of the last stage which does a
--   big sort.  So the minimum executor memory lets everything get by without too much
--   allocation, and the overhead enables the sort.

set spark.sql.shuffle.partitions=1024;

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
        concat('A', cast(sum(if(day=1, view_count, 0)) as string)),
        concat('B', cast(sum(if(day=2, view_count, 0)) as string)),
        concat('C', cast(sum(if(day=3, view_count, 0)) as string)),
        concat('D', cast(sum(if(day=4, view_count, 0)) as string)),
        concat('E', cast(sum(if(day=5, view_count, 0)) as string)),
        concat('F', cast(sum(if(day=6, view_count, 0)) as string)),
        concat('G', cast(sum(if(day=7, view_count, 0)) as string)),
        concat('H', cast(sum(if(day=8, view_count, 0)) as string)),
        concat('I', cast(sum(if(day=9, view_count, 0)) as string)),
        concat('J', cast(sum(if(day=10, view_count, 0)) as string)),
        concat('K', cast(sum(if(day=11, view_count, 0)) as string)),
        concat('L', cast(sum(if(day=12, view_count, 0)) as string)),
        concat('M', cast(sum(if(day=13, view_count, 0)) as string)),
        concat('N', cast(sum(if(day=14, view_count, 0)) as string)),
        concat('O', cast(sum(if(day=15, view_count, 0)) as string)),
        concat('P', cast(sum(if(day=16, view_count, 0)) as string)),
        concat('Q', cast(sum(if(day=17, view_count, 0)) as string)),
        concat('R', cast(sum(if(day=18, view_count, 0)) as string)),
        concat('S', cast(sum(if(day=19, view_count, 0)) as string)),
        concat('T', cast(sum(if(day=20, view_count, 0)) as string)),
        concat('U', cast(sum(if(day=21, view_count, 0)) as string)),
        concat('V', cast(sum(if(day=22, view_count, 0)) as string)),
        concat('W', cast(sum(if(day=23, view_count, 0)) as string)),
        concat('X', cast(sum(if(day=24, view_count, 0)) as string)),
        concat('Y', cast(sum(if(day=25, view_count, 0)) as string)),
        concat('Z', cast(sum(if(day=26, view_count, 0)) as string)),
        concat('[', cast(sum(if(day=27, view_count, 0)) as string)),
        concat('\\', cast(sum(if(day=28, view_count, 0)) as string)),
        concat(']', cast(sum(if(day=29, view_count, 0)) as string)),
        concat('^', cast(sum(if(day=30, view_count, 0)) as string)),
        concat('_', cast(sum(if(day=31, view_count, 0)) as string))
    ), '([A-Z]|[\\[\\]\\\\^\\_])0', '')
from ${source_table}
where
    year=${year}
    and month=${month}
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
