-- Stores various metrics from gdi sources as population data.
--
-- Parameters:
--     source_table         -- Read raw data from here
--     metrics_table        -- Read metrics data from here
--     country_data         -- Read country data from here
--     pivot_table          -- Read pivot data from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--
-- Usage:
--     hive -f population_leadership_input_metrics.hql                    \
--          -d source_table=gdi.population_data_input_metrics             \
--          -d metrics_table=gdi.geoeditor_online_input_metrics           \
--          -d country_data=gdi.country_meta_data                         \
--          -d pivot_table=gdi.geoeditor_input_metrics_pivot              \
--          -d destination_table=gdi.population_leadership_input_metrics  \
--          -d year=2021
--


WITH country_data AS (
       SELECT distinct country_code_iso_3,
          first_value(canonical_country_name) over(PARTITION BY country_code_iso_3)  country_name,
          country_code_iso_2
     FROM ${country_data} country
    WHERE country_code_iso_3 IS NOT NULL
), wikipedia_editors AS (
 SELECT country_code,wikipedia
  FROM ${pivot_table}
 WHERE metric = 'monthly_bins'
   AND year = ${year}
), average_editors AS (
  SELECT country_code,
         monthly_edits_avg,
         monthly_distinct_editors
    FROM ${metrics_table}
   WHERE year = ${year}
)
INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}')
SELECT country.country_code_iso_3,
       coalesce(population.population_annual_signal,0)                        population_annual_signal,
       coalesce(population.population_annual_change,0)                        population_annual_change,
       coalesce(wedit.wikipedia,0)                                            wikipedia_annual_signal,
       coalesce(avg_cal_year.monthly_edits_avg,coalesce(wedit.wikipedia,0))   monthly_edits_avg,
       coalesce(avg_cal_year.monthly_distinct_editors,0)                      monthly_distinct_editors,
       coalesce(population.gdp_per_capita_ppp_current,0)                      gdp_per_capita_ppp_current,
       coalesce(population.gdp_per_capita_ppp_constant,0)                     gdp_per_capita_ppp_constant,
       coalesce(population.internet_percent_annual_signal,0)                  internet_percent_annual_signal
  FROM country_data country
  LEFT JOIN ${source_table} population ON (country.country_code_iso_3 = population.country_code AND year = ${year} - 1)
  LEFT JOIN wikipedia_editors wedit ON (country.country_code_iso_2 = wedit.country_code)
  LEFT JOIN average_editors avg_cal_year ON (country.country_code_iso_2 = avg_cal_year.country_code);