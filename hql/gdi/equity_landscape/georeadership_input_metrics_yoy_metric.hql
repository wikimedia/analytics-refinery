-- Extracts data from the various gdi tables and stores the metrics as overall engagement
--
-- Parameters:
--     source_table              -- Read raw data from here
--     prior_metric              -- Read metrics from here
--     metric_table              -- Write metrics to here
--     destination_table         -- Insert results here
--     year                      -- YYYY to compute statistics for
--
-- Usage:
--     hive -f georeadership_input_metrics_yoy_metrics.hql                      \
--          -d source_table=gdi.georeadership_input_metrics                     \
--          -d prior_metric=pageviews                                           \
--          -d metric_table=yoy_pageviews                                       \
--          -d destination_table=gdi.georeadership_input_metrics                \
--          -d year=2021
--

DROP TABLE IF EXISTS ${source_table}_temp;

CREATE TABLE IF NOT EXISTS ${source_table}_temp AS  -- This done to prevent the cannot write from a source that's being read.
SELECT *
  FROM ${source_table}
  WHERE year IN  (${year} - 1, ${year})
    AND metric = '${prior_metric}';

WITH percentage_metrics AS
(

   SELECT country_code,
          metric_value / lag(metric_value) over (partition by country_code order by year) as yoy_change,
          year
          FROM ${source_table}_temp
         WHERE year IN (${year}-1,${year})
           AND metric = '${prior_metric}'

)
INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}', metric='${metric}')
SELECT country_code,
       yoy_change
  FROM percentage_metrics
  WHERE year = ${year};

DROP TABLE ${source_table}_temp;