CREATE EXTERNAL TABLE IF NOT EXISTS gdi.affiliate_leadership_input_metrics (
 `country_code`                           string    COMMENT '3-digit country code',
 `count_operating_affiliates`             int       COMMENT 'Total number of active affiliates in the country',
 `affiliate_size_max`                     int       COMMENT 'Maximum number of affiliates in the country',
 `affiliate_size_growth`                  int       COMMENT 'Growth in affiliate size',
 `affiliate_tenure_max`                   double    COMMENT 'Maximum tenure of an affiliate in the country',
 `governance_type`                        int       COMMENT 'Governance type of the affiliate represented as an integer, where, No structure = 0, Democratic Process/Consensus = 1 and Board = 2.',
 `affiliate_tenure_average`               double    COMMENT 'Average tenure of operating affiliates in the country'
)
COMMENT
  'Store metric on the leadership of affiliates in each country'
PARTITIONED BY (
  `year` int COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/affiliate_leadership_input_metrics'
;
