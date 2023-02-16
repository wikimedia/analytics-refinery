-- Stores various metrics from gdi sources as access input metrics.
--
-- Parameters:
--     mobile_index          -- Read mobile index data from here
--     country_data          -- Read country data from here
--     social_progress       -- Read social progress data from here
--     score_year            -- The year to use for the score
--     world_bank_year       -- The year to use for the world bank data
--     world_bank            -- Read world bank data from here
--     spi_year              -- The year to use for the spi data
--     destination_table     -- Insert results here
--     year                  -- YYYY to compute statistics for
--
-- Usage:
--     hive -f access_input_metrics.hql                                      \
--          -d mobile_index=gdi.mci_input_metrics                            \
--          -d country_data=gdi.country_meta_data                            \
--          -d social_progress=gdi.social_progress_input_metrics             \
--          -d population=gdi.population_data_input_metrics                  \
--          -d score_year=2019                                               \
--          -d world_bank_year=2020                                          \
--          -d spi_year=2020                                                 \
--          -d world_bank=gdi.world_bank_data_input_metrics                  \
--          -d destination_table=gdi.access_input_metrics                    \
--          -d year=2021
--

WITH country_data AS (
   SELECT distinct country_code_iso_3,
          first_value(canonical_country_name) over(PARTITION BY country_code_iso_3)  canonical_country_name,
          country_code_iso_2
     FROM ${country_data} country
    WHERE country_code_iso_3 IS NOT NULL
),

 world_bank AS (
   SELECT *
     FROM ${world_bank}
     WHERE time ='YR${world_bank_year}'
),
mobile_connectivity_index AS (

SELECT *
  FROM (
     SELECT iso_code,
            connectivity_index / lag(connectivity_index) over (partition by iso_code order by score_year)  as connectivity_index,
            connectivity_index as curr_year_value,
            lag(connectivity_index) over (partition by iso_code order by score_year) prev_year_value,
            score_year
       FROM ${mobile_index}
      WHERE score_year IN (${score_year}-1,${score_year})
    ) mci
  WHERE score_year = ${score_year}
)
INSERT OVERWRITE TABLE ${destination_table} PARTITION (year=${year})
SELECT country.country_code_iso_3,
       COALESCE(population.internet_percent_annual_signal,0)                                  internet_percent_annual_signal,
       COALESCE(mobile.value,0)                                                               mobile_subscriptions_annual_signal,
       COALESCE(social.access_to_basic_knowledge,0)                                           access_to_basic_knowledge_annual_signal,
       COALESCE(social.access_to_information,0)                                               access_to_information_annual_change,
       COALESCE(mci.connectivity_index,0)                                                     annual_connectivity_index_annual_change,
       coalesce(population.internet_annual_change,0)                                          internet_annual_change
  FROM country_data country
  LEFT JOIN world_bank mobile ON (country.country_code_iso_3 = mobile.economy AND mobile.series = 'IT.CEL.SETS.P2')
  LEFT JOIN ${social_progress} social ON (country.country_code_iso_3 = social.country_code AND social.year = ${year} AND social.spi_year = ${spi_year})
  LEFT JOIN ${population} population ON (country.country_code_iso_3 = population.country_code AND population.year = ${year} - 1)
  LEFT JOIN mobile_connectivity_index mci ON (country.country_code_iso_3 = mci.iso_code);
