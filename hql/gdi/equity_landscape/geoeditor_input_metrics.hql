-- Aggregates geoeditors_monthly data and groups them according to wiki_db_map classifications into geoeditor_metrics table.
--
-- Parameters:
--     source_table         -- Read raw data from here
--     map_table            -- Read wiki_db_map from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--
-- Usage:
--     hive -f geoeditor_input_metrics.hql                      \
--          -d source_table=wmf.geoeditors_monthly              \
--          -d map_table=gdi.wiki_db_map_input_metrics          \
--          -d canonical_wiki=canonical_data.wikis              \
--          -d destination_table=gdi.geoeditor_input_metrics    \
--          -d year=2021
--
WITH wikis AS  (
  SELECT DISTINCT *
  FROM  (
  SELECT database_code,
         database_group as grouped_bin
    FROM ${canonical_wiki}
  UNION
  SELECT database_code,
         grouped_bin
    FROM ${map_table}
  )
  WHERE grouped_bin in ('commons', 'mediawiki', 'wikidata', 'wikipedia','wikisource', 'sister_project', 'organizing_wiki')
)
INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}')
SELECT mon.country_code,
       sum(mon.distinct_editors)    AS distinct_editors,
       lower(wikis.grouped_bin)     AS grouped_bin,
       mon.month
  FROM wikis,
       ${source_table} mon
 WHERE mon.wiki_db = wikis.database_code
   AND mon.month like '${year}-%'
 GROUP BY mon.country_code, lower(wikis.grouped_bin), mon.month;