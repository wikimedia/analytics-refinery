-- Aggregate geoeditors_edits_monthly data into edits by country by year.
--   Do not show any countries with fewer than 100,000 edits per year
--   for privacy reasons.
--
-- Parameters:
--      project_map_table       -- Read project info from here
--      project_map_table_snapshot
--                              -- The partition to use from project_map_table
--      country_map_table       -- Read country info from here
--      source_table            -- Read monthly editors data from here
--      destination_directory   -- Write yearly files here
--      namespace_zero_edit_count_threshold
--                              -- The threshold of namespace-zero-edits over
--                              -- which a country is included in the report
--      year                    -- YYYY to compute statistics for
--      project_family          -- The project family for which the report is computed
--
-- Usage:
-- spark-sql  -f write_geoeditors_edits_yearly_data.hql                             \
--            -d project_map_table=wmf_raw.mediawiki_project_namespace_map          \
--            -d project_map_table_snapshot=2021-12                                 \
--            -d country_map_table=canonical_data.countries                         \
--            -d source_table=wmf.geoeditors_edits_monthly                          \
--            -d destination_directory=/wmf/tmp/analytics/geoeditors/edits/yearly   \
--            -d namespace_zero_edit_count_threshold=100000                         \
--            -d year=2021                                                          \
--            -d project_family=wikipedia
--
-- Note: This SQL script only works on Spark 3.X.

   with output as (
 select c.name as country,
        sum(edit_count) as edits,
        sum(namespace_zero_edit_count) as namespace_zero_edits

   from ${source_table} g
            inner join
        (select distinct dbname
           from ${project_map_table}
          where hostname like '%.${project_family}.%'
            and snapshot='${project_map_table_snapshot}'
        ) w                                 on w.dbname = g.wiki_db
            inner join
        ${country_map_table} c              on g.country_code = c.iso_code

  where month like '${year}-%'

  group by c.name
 having sum(namespace_zero_edit_count) >= ${namespace_zero_edit_count_threshold}
  order by country
  limit 10000
)

 INSERT OVERWRITE DIRECTORY '${destination_directory}'
 USING CSV
 OPTIONS ('header' 'false', 'compression' 'none')

 select
 /*+ COALESCE(1) */
        country,
        edits,
        namespace_zero_edits
   from output
;