-- Calculates the year-on-year change for the previous year and the current year from the geoeditor_metric_pivot table per country
--
-- Parameters:
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--     metric               -- Metric to compute statistics for
--
-- Usage:
--     hive -f geoeditor_input_metrics_pivot_yoy_change.hql             \
--          -d source_table=gdi.geoeditor_input_metrics_pivot           \
--          -d destination_table=gdi.geoeditor_input_metrics_pivot      \
--          -d year=2021
--          -d metric=yoy_change
--

DROP TABLE IF EXISTS ${source_table}_temp;

CREATE TABLE IF NOT EXISTS ${source_table}_temp AS -- This done to prevent the cannot write from a source that's being read.
SELECT *
  FROM ${source_table}
  WHERE year IN  (${year} - 1, ${year})
    AND metric = 'monthly_bins';

INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}', metric='${metric}')
SELECT country_code,
       commons,
       mediawiki,
       wikidata,
       wikipedia,
       wikisource,
       sister_project,
       organizing_wiki
  FROM (
        SELECT  country_code,
                commons / lag(commons) over (partition by country_code order by year)                 as commons,
                mediawiki / lag(mediawiki) over (partition by country_code order by year)             as mediawiki,
                wikidata/ lag(wikidata) over (partition by country_code order by year)                as wikidata,
                wikipedia /lag(wikipedia) over (partition by country_code order by year)              as wikipedia,
                wikisource /lag(wikisource) over (partition by country_code order by year)            as wikisource,
                sister_project / lag(sister_project) over (partition by country_code order by year)   as sister_project,
                organizing_wiki / lag(organizing_wiki) over (partition by country_code order by year) as organizing_wiki,
                year
          FROM ${source_table}_temp
         WHERE year IN (${year}-1,${year})
          AND metric = 'monthly_bins'
    ) data
  WHERE year = ${year};

DROP TABLE ${source_table}_temp;
