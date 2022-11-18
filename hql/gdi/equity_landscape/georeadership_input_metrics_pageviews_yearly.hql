-- Aggregates data from pageview_hourly
--
-- Parameters:
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--     metric               -- Metric to compute statistics for
--
-- Usage:
--     hive -f georeadership_input_metrics_pageviews_yearly.hql           \
--          -d source_table=wmf.pageview_hourly                           \
--          -d destination_table=gdi.georeadership_input_metrics          \
--          -d year=2021
--          -d metric=pageviews
--

INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}', metric='${metric}')
SELECT country_code,
       sum(view_count ) as sum_view_count
  FROM ${source_table}
 WHERE year = ${year}
   AND agent_type != 'spider'
 GROUP BY country_code;