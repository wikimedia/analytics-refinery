-- Aggregates data from geoeditors_monthly table to create active editors metrics
--
-- Parameters:
--     source_table         -- Read raw data from here
--     geoeditor_metrics    -- Read geoeditor_input_metrics from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--
-- Usage:
--     hive -f geoeditor_online_input_metrics.hql                         \
--          -d source_table=wmf.geoeditors_monthly                        \
--          -d country_data=gdi.country_meta_data                         \
--          -d destination_table=gdi.geoeditor_online_input_metrics       \
--          -d year=2021
--
WITH country_data AS (
       SELECT distinct country_code_iso_3,
          first_value(canonical_country_name) over(PARTITION BY country_code_iso_3)  country_name,
          country_code_iso_2
     FROM ${country_data} country
    WHERE country_code_iso_3 IS NOT NULL
),
average_active_editors AS (
SELECT country_code,
       SUM(distinct_editors) / count(distinct month) monthly_distinct_editors
  FROM ${source_table}
 WHERE month like '${year}-%'
   AND activity_level != '1 to 4'
 GROUP BY country_code
),monthly_edits AS (
  SELECT data.country_code,
         AVG(data.distinct_editors) monthly_avg_editors
    FROM (
            SELECT country_code,
                   SUM(distinct_editors) distinct_editors,
                   month
              FROM ${source_table}
             WHERE month like '${year}-%'
             GROUP BY country_code,
                      month
         ) data
  GROUP BY data.country_code
)
INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}')
SELECT country.country_code_iso_2,
       act.monthly_distinct_editors,
       monthly.monthly_avg_editors,
       IF(monthly.monthly_avg_editors =0,0,act.monthly_distinct_editors/monthly.monthly_avg_editors)  as percent_editors_active
  FROM country_data country
  LEFT JOIN average_active_editors act ON (country.country_code_iso_2 = act.country_code)
  LEFT JOIN monthly_edits monthly ON (country.country_code_iso_2 = monthly.country_code);
