CREATE EXTERNAL TABLE IF NOT EXISTS gdi.grants_leadership_input_metrics (
  `country_code`                                 string      COMMENT 'The 2-letter ISO country code where the device is geolocated to, including Unknown (--)',
  `sum_historical_grants_to_date`                double      COMMENT 'Sum of all Wikimedia Foundation Grants awarded from 2010 through referenced year in USD ($)',
  `sum_calendar_year_grants`                     double      COMMENT 'Sum of all Wikimedia Foundation Grants awarded in referenced calendar year in USD ($)',
  `count_historical_grants_to_date`              int         COMMENT 'Number of historical grants to date',
  `count_calendar_year_grants`                   int         COMMENT 'Number of grants in the calendar year',
  `annual_grants_weighted`                       double      COMMENT 'sum_historical_grants_to_date divided by the country\'s GDP per capita, PPP (current international $)',
  `historical_grants_weighted`                   double      COMMENT 'sum_calendar_year_grants divided by the country\'s GDP per capita, PPP (constant international $)',
  `annual_grants_change_rate`                    double      COMMENT 'Change in Grants sum over the past 5 calendar years averaged annually (i.e., 5-year growth divided by 5)',
  `annual_grants_by_annual_change_rate`          double      COMMENT 'The weighted annual grants value (annual_grants_weighted) multiplied by the average annual growth rate (annual_grants_change_rate)',
  `historical_grants_by_annual_change_rate`      double      COMMENT 'The weighted historical grants value (historical_grants_weighted) multiplied by the average annual growth rate (annual_grants_change_rate)',
  `sum_calendar_year_affiliate_grants`           double      COMMENT 'Sum of all Wikimedia Foundation Grants awarded in referenced calendar year to affiliates in USD ($)',
  `sum_historical_affiliate_grants`              double      COMMENT 'Sum of all Wikimedia Foundation Grants awarded from 2010 through referenced year to affiliates in USD ($)',
  `count_calendar_year_affiliate_grants`         int         COMMENT 'Number of grants in the calendar year to affiliates',
  `count_historical_affiliate_grants`            int         COMMENT 'Number of historical grants to date to affiliates',
  `annual_affiliate_grants_weighted`             double      COMMENT 'The total annual value in USD for all WMF grant awarded to officially recognized affiliates weighted by GDP per capita PPP current, adjusted for inflation',
  `historical_affiliate_grants_weighted`         double      COMMENT 'sum_historical_grants_to_date_affiliates divided by the country\'s GDP per capita, PPP (constant international $)'
)
COMMENT
  'Stores grants leadership input metrics for each country'
PARTITIONED BY (
  `year`      int    COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/grants_leadership_input_metrics'
;