CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`grants_input_metrics` (
  `fiscal_year_ending`              int            COMMENT 'The end of Wikimedia Foundation\'s fiscal year which falls in July',
  `program`                         string         COMMENT 'Label identifying associated grant program active for the grant',
  `request_id`                      string         COMMENT 'The Fluxx database grant request identification number',
  `usd_over_grant_life`             double         COMMENT 'The US Dollar amount distributed over the life of the grant that calendar year.',
  `gender_gap`                      string         COMMENT 'Identifies if the grant was targeting the gender gap.',
  `calendar_year`                   int            COMMENT 'The Gregeorian calendar year (ending December 31), associated with the data',
  `country_name`                    string         COMMENT 'Fluxx database country label',
  `country_code`                    string         COMMENT 'Three-letter country code using the ISO 3166-1 standard',
  `org_type`                        string         COMMENT 'Type of grantee in terms of individual, affiliate by type, or other non-affiliate organization.',
  `un_subcontinent`                 string         COMMENT 'The subcontinent description according to the UN',
  `un_continent`                    string         COMMENT 'The continent description according to the UN',
  `population`                      double         COMMENT 'Corresponding annual population (in thousands)',
  `gdp_per_capita_constant`         double         COMMENT 'Location\'s GDP per capita PPP divided by the average country\'s population for the same year and expressed in constant 2017 international dollars',
  `gdp_per_capita_current`          double         COMMENT 'Location\'s GDP per capita PPP, divided by the average country\'s population for the same year and expressed in international dollars of the reference year',
  `ppp_conversion_rate`             double         COMMENT 'PPP conversion factor, GDP (LCU per int\'l dollars)',
  `dollar_weighted_constant`        double         COMMENT 'usd_over_grant_life divided by gdp_per_capita_constant',
  `dollar_weighted_current`         double         COMMENT 'usd_over_grant_life divided by gdp_per_capita_current',
  `grant_value`                     double         COMMENT 'This is the purchasing power parity (PPP) value of grants in the local economy, converted from USD to the local currency units, and taking into account inflation.',
  `inflation_rate`                  double         COMMENT 'The inflation rate for the country in the reference year (%)',
  `gdp_constant_pop_weighted`       double         COMMENT 'usd_over_grant_life divided by gdp_per_capita_constant and population',
  `dec_adjusted_exchange_rate`      double         COMMENT 'The official exchange rate reported in the IMF\'s International Financial Statistics, expressed as number of Local Currency Units (LCU) per USD.',
  `grant_making_region`             string         COMMENT 'Country grouping schema aligned to the Wikimedia Foundation\'s grant-making committees.',
  `converted_usd_per_1000_pop`      double         COMMENT 'The per population portion of the US dollar value in the local economy',
  `multi_year_grant`                string         COMMENT 'Identifies whether the grant is a single year or multi-year grant',
  `location_self_disclosed`         string         COMMENT 'Identifies whether the grant country location was identified in the meta page title or not',
  `blocklisted`                     string         COMMENT 'Identifies whether the grant country location is flagged for data privacy',
  `status`                          string         COMMENT 'The status of the grant at the close of the reference year.',
  `unique_grantee_id`               string         COMMENT 'Randomized assigned unique ID numbers for individual grantees'
  )
COMMENT
  'The table containing the grants data'
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/gdi/grants_input_metrics';
