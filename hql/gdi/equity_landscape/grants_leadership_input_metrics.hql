-- Extracts data from the gdi.grants table and stores the transformations
--
-- Parameters:
--     source_table         -- Read raw data from here
--     world_bank_data      -- Read data from world_bank from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--
-- Usage:
--     hive -f grants_leadership_input_metrics.hql                    \
--          -d source_table=gdi.grants_input_metrics                  \
--          -d destination_table=gdi.grants_leadership_input_metrics  \
--          -d year=2021
--
WITH grant_totals_by_year_and_country AS (
   SELECT country_code,
          round(sum(cast(coalesce(usd_over_grant_life,0) as double)),2) AS total_grants_calendar_year,
          count(1)                                                      AS count_grants_calendar_year,
          cast(calendar_year as int) calendar_year
     FROM ${source_table}
    WHERE year = ${year}
      AND cast(calendar_year as int) <= ${year}
     GROUP BY country_code, calendar_year
 ),
  total_historical_grants_by_country AS (
    SELECT country_code,
           sum(total_grants_calendar_year)          AS total_historical_grants,
           count(count_grants_calendar_year)        AS historical_grant_count
       FROM grant_totals_by_year_and_country
     GROUP BY country_code
 ),
  historical_grants_weighted AS (
    SELECT country_code,
           sum(dollar_weighted_constant)                            AS total_historical_grants_weighted,
           sum(if(calendar_year=${year},dollar_weighted_current,0)) AS total_ann_grants_weighted
      FROM ${source_table}
     WHERE year = ${year}
       AND calendar_year <= ${year}
     GROUP BY country_code
 ),
 affiliate_led_grant_totals_by_year_and_country AS (
   SELECT country_code,
          round(sum(cast(coalesce(usd_over_grant_life ,0)  as double)),2) AS total_calendar_year_grants,
          count(1)                                                        AS count_affiliate_led_grants_cal_year,
          cast(calendar_year as int) calendar_year
     FROM ${source_table}
    WHERE year = ${year}
      AND lower(org_type) IN ('chapter', 'thematic organization' ,'user group')
    GROUP BY country_code, calendar_year
 ),
  total_historical_affiliate_led_grants_by_country AS (
   SELECT country_code,
          SUM(total_calendar_year_grants)                    AS total_historical_grants_to_date,
          count(count_affiliate_led_grants_cal_year)         AS count_affiliate_led_grants_hist
     FROM affiliate_led_grant_totals_by_year_and_country
    WHERE calendar_year <= ${year}
    GROUP BY country_code
 ),
 historical_affiliate_grants_weighted AS (
    SELECT country_code,
           sum(dollar_weighted_constant)                            AS total_historical_affiliate_grants_weighted,
           sum(if(calendar_year=${year},dollar_weighted_current,0)) AS total_ann_affiliate_grants_weighted
      FROM ${source_table}
     WHERE year = ${year}
       AND calendar_year <= ${year}
       AND lower(org_type) IN ('chapter', 'thematic organization' ,'user group')
     GROUP BY country_code
 ),
average_grant_totals_last_5_years_by_country AS (
   SELECT country_code,
          avg(total_grants_calendar_year)   AS average_grants
     FROM grant_totals_by_year_and_country
    WHERE calendar_year BETWEEN ${year} - 4 AND ${year}
    GROUP BY country_code
),
average_grant_totals_previous_5_years_by_country AS
 (

   SELECT country_code,
          avg(total_grants_calendar_year)   average_grants
     FROM grant_totals_by_year_and_country
    WHERE calendar_year BETWEEN ${year} - 9 AND ${year} - 5
    GROUP BY country_code
 )
,

grant_loss_comparing_last_5_years_to_previous_5_years AS (

  SELECT country.country_code,
         grant_5.average_grants                         AS average_grants_5,
         grant_10.average_grants                        AS average_grants_10,
         grant_10.average_grants/grant_5.average_grants AS avg_ann_5_yr_change
    FROM total_historical_grants_by_country country
    LEFT JOIN average_grant_totals_last_5_years_by_country grant_10 ON(country.country_code = grant_10.country_code)
    LEFT JOIN average_grant_totals_previous_5_years_by_country grant_5 ON (country.country_Code = grant_5.country_code)

),
five_year_grant_loss_or_gain_by_country AS (

   SELECT country_code,
          average_grants_5,
          average_grants_10,
          round(avg_ann_5_yr_change,2)        AS average_change_5,
          round(-1 + avg_ann_5_yr_change,2)   AS loss_or_gain
     FROM grant_loss_comparing_last_5_years_to_previous_5_years
),
grants_data AS (
SELECT hist.total_historical_grants historical_grants_to_date,
       hist.country_code,
       total_grants.total_grants_calendar_year,
       hist.historical_grant_count,
       total_grants.count_grants_calendar_year,
       hist_w.total_historical_grants_weighted,
       hist_w.total_ann_grants_weighted
  FROM total_historical_grants_by_country hist
  LEFT JOIN grant_totals_by_year_and_country total_grants ON (hist.country_code = total_grants.country_code AND total_grants.calendar_year = ${year})
  LEFT JOIN historical_grants_weighted hist_w ON (hist.country_code = hist_w.country_code)
 ),  final_grants_data AS (
SELECT gdata.country_code,
       gdata.total_grants_calendar_year,
       gdata.historical_grants_to_date,
       gdata.historical_grant_count,
       gdata.count_grants_calendar_year,
       1+(coalesce(lorg.loss_or_gain,0)/5)   AS loss_or_gain_five_year,
       gdata.total_historical_grants_weighted,
       gdata.total_ann_grants_weighted
  FROM grants_data gdata
  LEFT JOIN five_year_grant_loss_or_gain_by_country lorg ON (lorg.country_code = gdata.country_code)
)
INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}')
 SELECT *
 FROM  (
 SELECT data.country_code,
        coalesce(data.historical_grants_to_date,0)                                     AS total_historical_grants_to_date,
        coalesce(data.total_grants_calendar_year,0)                                    AS total_calendar_year_grants,
        coalesce(data.historical_grant_count,0)                                        AS count_historical_grants_to_date,
        coalesce(data.count_grants_calendar_year,0)                                    AS count_calendar_year_grants,
        coalesce(data.total_ann_grants_weighted,0)                                     AS total_annual_grants_presence_weighted,
        coalesce(data.total_historical_grants_weighted,0)                              AS total_historical_grants_presence_weighted,
        coalesce(data.loss_or_gain_five_year,0)                                        AS five_year_grants_change,
        coalesce(data.total_ann_grants_weighted * loss_or_gain_five_year,0)            AS total_ann_grants_five_year_change,
        coalesce(data.total_historical_grants_weighted * loss_or_gain_five_year,0)     AS total_historical_grants_five_year_change,
        coalesce(cal.total_calendar_year_grants,0)                                     AS total_annual_grants_annual_signal,
        coalesce(hist.total_historical_grants_to_date,0)                               AS total_historical_grants_annual_signal,
        coalesce(cal.count_affiliate_led_grants_cal_year,0)                            AS count_affiliate_led_grants_annual_signal,
        coalesce(hist.count_affiliate_led_grants_hist,0)                               AS count_historical_affiliate_led_grants_annual_signal,
        coalesce(hist_w.total_ann_affiliate_grants_weighted,0)                         AS total_annual_affiliate_led_grants_presence_weighted,
        coalesce(hist_w.total_historical_affiliate_grants_weighted,0)                  AS total_historical_affiliate_led_grants_presence_weighted
   FROM final_grants_data data
   LEFT JOIN affiliate_led_grant_totals_by_year_and_country cal ON (data.country_code = cal.country_code AND cal.calendar_year = ${year})
   LEFT JOIN total_historical_affiliate_led_grants_by_country hist ON (data.country_code = hist.country_code)
   LEFT JOIN historical_affiliate_grants_weighted hist_w ON (data.country_code = hist_w.country_code)
   ) data
  WHERE data.country_code IS NOT NULL