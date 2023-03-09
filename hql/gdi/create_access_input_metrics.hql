CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`access_input_metrics` (
  `country_code`                              string         COMMENT 'The 3-digit IBAN country code the affiliate belongs to',
  `internet_percent_annual_signal`            double         COMMENT 'The percentage of the population that uses the internet',
  `mobile_subscriptions_annual_signal`        double         COMMENT 'The number of mobile cellular subscriptions',
  `access_to_basic_knowledge_annual_signal`   double         COMMENT 'The percentage of the population that has access to basic knowledge',
  `access_to_information_annual_signal`       double         COMMENT 'The percentage of the population that has access to information',
  `annual_connectivity_index_annual_change`   double         COMMENT 'Connectivity index annual change',
  `internet_annual_change`                    double         COMMENT 'Internet access annual change'
  )
COMMENT  'Contains access input metrics'
PARTITIONED BY (
  `year` int COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/access_input_metrics'
;