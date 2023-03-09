CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`population_data_input_metrics` (
  `country_name`                    string  COMMENT 'Country name',
  `country_code`                    string  COMMENT 'ISO 3166-1 alpha-3 country code',
  `gdp_per_capita_ppp_current`      double  COMMENT 'GDP per capita (PPP) (current international $)',
  `gdp_per_capita_ppp_constant`     double  COMMENT 'GDP per capita (PPP) (constant 2011 international $)',
  `population_annual_signal`        bigint  COMMENT 'Population annual signal',
  `population_annual_change`        double  COMMENT 'Population change (annual %)',
  `internet_percent_annual_signal`  double  COMMENT 'Internet users percentage of population',
  `internet_annual_change`          double  COMMENT 'Internet users change (annual %)'
)
COMMENT
  'Population data input metrics'
PARTITIONED BY (`year`      int    COMMENT 'The year in YYYY format')
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/population_data_input_metrics'
;
