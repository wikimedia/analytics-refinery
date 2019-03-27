-- Aggregate geoeditors_daily data into edits by country by year.
--   Do not show any countries with fewer than 100,000 edits per year
--   for privacy reasons.
--
-- Parameters:
--      project_map_table       -- Read project info from here
--      country_map_table       -- Read country info from here
--      source_table            -- Read monthly editors data from here
--      destination_directory   -- Write yearly files here
--      edit_count_lower_bound  -- The lowest number of edits that qualifies a country to be in the yearly report
--      year                    -- YYYY to compute statistics for
--
-- Usage:
--     hive -f write_geoeditors_edits_yearly_data.hql                    \
--         -d project_map_table=wmf_raw.mediawiki_project_namespace_map  \
--         -d country_map_table=wmf.country_info                         \
--         -d source_table=wmf.geoeditors_edits_monthly                  \
--         -d destination_directory=/tmp/archive/geoeditors/edits/yearly \
--         -d edit_count_lower_bound=100000                              \
--         -d year=2018
--

SET hive.exec.compress.output = false;
SET mapred.reduce.tasks       = 1;


   with output as (
 select c.country,
        sum(edit_count) as edits,
        sum(namespace_zero_edit_count) as namespace_zero_edits

   from ${source_table} g
            inner join
        (select distinct dbname
           from ${project_map_table}
          where hostname like '%.wikipedia.%'
            and snapshot='${year}-12'
        ) w                                 on w.dbname = g.wiki_db
            inner join
        ${country_map_table} c              on g.country_code = c.country_code

  where month like '${year}-%'

  group by c.country
 having sum(edit_count) >= ${edit_count_lower_bound}
  order by country
  limit 10000
)

 INSERT OVERWRITE DIRECTORY '${destination_directory}/${year}'

 select concat_ws(',',
            country,
            cast(edits as string),
            cast(namespace_zero_edits as string)
        )
   from output
;

dfs -mv ${destination_directory}/${year}/000000_0 ${destination_directory}/${year}.csv;
dfs -rm -r ${destination_directory}/${year};
