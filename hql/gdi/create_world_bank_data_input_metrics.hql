CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`world_bank_data_input_metrics` (
  `value`                double  COMMENT 'The value of the indicator',
  `economy`              string  COMMENT 'The economy (country_code) of the indicator being measured',
  `aggregate`            boolean COMMENT 'Denotes whether the indicator is an aggregate or not',
  `time`                 string  COMMENT 'The time period of the indicator being measured in YRYYYY format'
)
COMMENT
  'This table contains data from the World Bank Data API.'
PARTITIONED BY (
  `series`      string    COMMENT 'The series of the indicator being measured'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/world_bank_data_input_metrics'
;
