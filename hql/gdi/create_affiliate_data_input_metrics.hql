CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`affiliate_data_input_metrics` (
  `affiliate_code`                    string          COMMENT 'The unique and official alpha-code assigned to the affiliate. ',
  `affiliate_name`                    string          COMMENT 'The officially recognized English name of the affiliate and, if applicable, their name in their local community language.',
  `affiliate_country`                 string          COMMENT 'The description of the country or countries where the affiliate has organized - For international operations see operating_countries vs primary_country_code',
  `languages`                         array<string>   COMMENT 'The affiliate\'s reported operating languages (three-letter language code), drawn from affiliate form reporting at each calendar-year end',
  `affiliate_type`                    string          COMMENT 'Description of the affiliate type (Chapter, Thematic Organization, or Wikimedia User Group)',
  `start_date`                        date            COMMENT 'The affiliate\'s official recognition date based on the contract signed with the Wikimedia Foundation',
  `affiliate_tenure`                  double          COMMENT 'The total number of years between the affiliate\'s start_date and the referenced year',
  `member_count`                      int             COMMENT 'Annual reported number of members of an affiliate via the Wikimedia Affiliates Data Portal or linked membership listings on meta.',
  `affiliate_size`                    int             COMMENT 'Ordinal description of the affiliate\'s size in terms of membership (number of members) ordinally coded 1 through 5',
  `affiliate_size_annual_change`      int             COMMENT 'The annual change in the affiliate_size ordinal category based on the change in the affiliate\'s number of members',
  `governance_type_description`       string          COMMENT 'The description of the affiliate\'s type of governance',
  `governance_type`                   int             COMMENT 'The ordinal value of the governance type',
  `country_code`                      string          COMMENT 'The primary country of the affiliate',
  `operating_countries`               array<string>   COMMENT 'ISO 3166-1 3-letter country codes of all recently* reported operating locations - data are as up-to-date as the affiliate form reporting at each year end (December 31).',
  `governance_annual_change`          int             COMMENT 'The annual change in the affiliate\'s mode of governance',
  `total_annual_change`               int             COMMENT 'The overall change in affiliate membership size and governance, the sum of the governance_annual_change and affiliate_size_annual_change',
  `member_editors`                    int             COMMENT 'The number of editors of the affiliate'
  )
COMMENT
  'Affiliate data input metrics'
PARTITIONED BY (
  `year` int COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/gdi/affiliate_data_input_metrics';
