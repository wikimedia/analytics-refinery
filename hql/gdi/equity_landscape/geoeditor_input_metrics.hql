-- Aggregates geoeditors_monthly data and groups them according to wiki_db_map classifications into geoeditor_metrics table.
--
-- Parameters:
--     source_table         -- Read raw data from here
--     map_table            -- Read wiki_db_map from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--
-- Usage:
--     hive -f geoeditor_input_metrics.hql                     \
--          -d source_table=wmf.geoeditors_monthly             \
--          -d map_table=gdi.wiki_db_map_input_metrics         \
--          -d destination_table=gdi.geoeditor_input_metrics    \
--          -d year=2021
--

INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}')
SELECT mon.country_code,
       sum(mon.distinct_editors) as distinct_editors,
       lower(wdb.grouped_bin)    as grouped_bin,
       mon.month
  FROM ${map_table} wdb
  JOIN ${source_table} mon ON (mon.wiki_db = wdb.database_code AND mon.month like '${year}-%')
 WHERE lower(wdb.grouped_bin) in ('commons', 'mediawiki', 'wikidata', 'wikipedia','wikisource', 'sister_project', 'organizing_wiki')
 GROUP BY mon.country_code, lower(wdb.grouped_bin), mon.month;