-- Stores affiliate leadership data based on multiple gdi sources.
--
-- Parameters:
--     affiliate_data       -- Read affiliate data from here
--     country_data         -- Read country data from here
--     grants_leadership    -- Read grants leadership data from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--
-- Usage:
--     hive -f affiliate_leadership_input_metrics.hql                         \
--          -d affiliate_data=gdi.affiliate_data_input_metrics                \
--          -d country_data=gdi.country_meta_data                             \
--          -d grants_leadership=gdi.grants_leadership_input_metrics          \
--          -d destination_table=gdi.affiliate_leadership_input_metrics       \
--          -d year=2021
--
WITH country_data AS (
   SELECT distinct iso3_country_code,
          first_value(country_area_label) over(PARTITION BY iso3_country_code)  country_area_label,
          iso2_country_code
     FROM ${country_data} country
    WHERE iso3_country_code IS NOT NULL
),

csv_affiliates AS (
SELECT country_code,
       count(1)                                                 count_affiliates_in_country,
       max(affiliate_size)                                      affiliate_max_size,
       datediff(to_date('${year}-12-31'),min(start_date))/365   total_affiliate_tenure,
       max(size_growth)                                         size_growth,
       max(governance_type)                                     governance_type
  FROM ${affiliate_data}
 WHERE year = ${year}
GROUP BY country_code

)
INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}')
SELECT metr.country_code,
       metr.count_affiliates_in_country,
       metr.affiliate_max_size,
       metr.size_growth,
       metr.total_affiliate_tenure,
       grants.total_calendar_year_grants,
       grants.total_calendar_year_grants/grants.total_calendar_year_grants_affiliates       percent_annual_grants_affiliate,
       grants.total_historical_grants_to_date/grants.total_historical_grants_affiliates     percent_historical_grants_affiliate,
       grants.total_historical_grants_affiliates,
       grants.count_calendar_year_grants,
       grants.count_historical_grants_to_date,
       metr.governance_type
  FROM csv_affiliates metr
 LEFT JOIN ${grants_leadership} grants ON (metr.country_code = grants.country_code AND grants.year = ${year});