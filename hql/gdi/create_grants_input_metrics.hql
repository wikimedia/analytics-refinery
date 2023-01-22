CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`grants_input_metrics` (
  `fiscal_year_ending`              int            COMMENT 'Fiscal year ending in (YYYY) format',
  `program`                         string         COMMENT 'Grant program',
  `request_id`                      string         COMMENT 'Request ID for grant',
  `usd_over_grant_life`             double         COMMENT 'The USD amount of the grant over its life (adjusted to annual period for multi-year grants)',
  `gender_gap`                      string         COMMENT 'Yes/No if there is a gender gap',
  `calendar_year`                   int            COMMENT 'Grant calendar year in (YYYY) format',
  `country_name`                    string         COMMENT 'The country name in full',
  `country_code`                    string         COMMENT 'The 3-digit IBAN country code',
  `org_type`                        string         COMMENT 'Grant organization type',
  `un_subcontinent`                 string         COMMENT 'UN classification of subcontinent for the grant program',
  `un_continent`                    string         COMMENT 'UN classification of continent for the grant program',
  `population`                      double         COMMENT 'The population of the country for a given year in thousands',
  `gdp_per_capita_constant`         double         COMMENT 'GDP per capita, PPP constant (2017 international $)',
  `gdp_per_capita_current`          double         COMMENT 'GDP per capita, PPP (current international $)',
  `ppp_conversion_rate`             double         COMMENT 'PPP conversion rate for the country for a given year, LCU per international $',
  `dollar_weighted_constant`        double         COMMENT 'Dollar-weighted GDP per capita, PPP constant (2017 international $)',
  `dollar_weighted_current`         double         COMMENT 'Dollar-weighted GDP per capita, PPP (current international $)',
  `dollar_conversion_factor`        double         COMMENT 'Dollars multiplied by DEC alternative conversion factor (LCU/USD) / PPP conversion factor',
  `inflation_rate`                  double         COMMENT 'Inflation rate, average consumer prices (annual %)',
  `gdp_constant_pop_weighted`       double         COMMENT 'GDP, PPP constant (2017 international $), population-weighted',
  `dec_adjusted_exchange_rate`      double         COMMENT 'DEC adjusted exchange rate (LCU/USD)',
  `grant_making_region`             string         COMMENT 'Grant making region',
  `converted_usd_per_1000_pop`      double         COMMENT 'USD per 1000 population',
  `multi_year_grant`                string         COMMENT 'Specify if the grant is multi-year or not',
  `location_self_disclosed`         string         COMMENT 'Specify if the grant location is self-disclosed or not',
  `blocklisted`                     string         COMMENT 'Specify if the grant is the country is blocklisted or not',
  `status`                          string         COMMENT 'Status of the grant',
  `unique_grantee_id`               string         COMMENT 'Unique grantee ID'
  )
COMMENT
  'The table containing the grants data'
PARTITIONED BY (
  `year` int COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/gdi/grants_input_metrics';
