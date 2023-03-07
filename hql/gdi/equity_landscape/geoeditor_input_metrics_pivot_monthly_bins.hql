-- Pivots the geoeditor_metrics table and performs aggregation on the columns
--
-- Parameters:
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--     metric               -- Metric to compute statistics for
--
-- Usage:
--     hive -f geoeditor_input_metrics_pivot_monthly_bins.hql             \
--          -d source_table=gdi.geoeditor_input_metrics                   \
--          -d destination_table=gdi.geoeditor_input_metrics_pivot        \
--          -d year=2021
--          -d metric=monthly_bins
--

WITH pivot_data AS (
 SELECT *
   FROM
   (
      SELECT country_code, grouped_bin, distinct_editors
        FROM ${source_table}
        WHERE year=${year}
   ) data
   PIVOT (
         CAST (round(avg(distinct_editors),2) AS DECIMAL (38,18))
         FOR grouped_bin IN
            (

         'commons','mediawiki','wikidata','wikipedia','wikisource','sister_project','organizing_wiki'
      )
   )
)
INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}', metric='${metric}')
SELECT *
  FROM pivot_data;