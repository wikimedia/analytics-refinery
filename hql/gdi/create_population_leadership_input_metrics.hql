CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`population_leadership_input_metrics` (
  `country_code`                   string  COMMENT 'ISO 3166-1 alpha-3 country code',
  `population_annual_signal`       bigint  COMMENT 'Population in 1000s',
  `population_annual_change`       double  COMMENT 'Population growth rate',
  `wikipedia_annual_signal`        double  COMMENT 'Wikipedia editors',
  `wikimedia_annual_signal`        double  COMMENT 'Wikimedia overall editors',
  `wikimedia_active_annual_signal` double  COMMENT 'Wikimedia active editors',
  `gdp_per_capita_ppp_current`     double  COMMENT 'GDP per capita (PPP) (current international $)',
  `gdp_per_capita_ppp_constant`    double  COMMENT 'GDP per capita (PPP) (constant 2011 international $)',
  `internet_percent_annual_signal` double  COMMENT 'Internet users percentage of population'
)
COMMENT
  'Population leadership input metrics'
PARTITIONED BY (`year`      int    COMMENT 'The year in YYYY format')
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/population_leadership_input_metrics'
;
