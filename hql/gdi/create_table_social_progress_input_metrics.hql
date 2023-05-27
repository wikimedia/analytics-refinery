CREATE EXTERNAL TABLE IF NOT EXISTS gdi.social_progress_input_metrics (
  `country_name`                                              string             COMMENT 'Name of the country.',
  `country_code`                                              string             COMMENT 'Country code',
  `spi_year`                                                  int                COMMENT 'Year of ranking.',
  `status`                                                    string             COMMENT 'Status of ranking for country',
  `spi_rank`                                                  int                COMMENT 'Social Progress Index rank',
  `social_progress_index`                                     double             COMMENT 'Social Progress Index',
  `access_to_basic_knowledge`                                 double             COMMENT 'Access to basic knowledge',
  `access_to_information_and_communication`                   double             COMMENT 'Access to information and communication'
)
COMMENT
  'Social Progress Index data from https://www.socialprogress.org/'
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/gdi/social_progress_input_metrics';