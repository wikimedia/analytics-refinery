CREATE EXTERNAL TABLE IF NOT EXISTS gdi.georeadership_input_metrics (
  `country_code`                string      COMMENT 'The 2-letter ISO country code where the device is geolocated to, including Unknown (--)',
  `metric_value`                double      COMMENT 'The aggregate of all the values of the metric'
)
COMMENT
  'Takes in the raw data from the wmf.unique_devices_per_domain_monthly and wmf.pageviews tables and aggregates the data by year and country code'
PARTITIONED BY (
  `year`      int    COMMENT 'The year in YYYY format',
  `metric`    string COMMENT 'The metric being aggregated. E.g. unique_devices/pageviews'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/georeadership_input_metrics'
;