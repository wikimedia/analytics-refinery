CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`country_meta_data` (
  `country_area_label`              string         COMMENT 'The country or area name',
  `iso3_country_code`               string         COMMENT 'The 3-digit IBAN country code the affiliate belongs to',
  `iso2_country_code`               string         COMMENT 'The ISO 3166-1 alpha-2 country code',
  `iban_label`                      string         COMMENT 'The IBAN country label',
  `grant_label`                     string         COMMENT 'The grant country label',
  `unesco_label`                    string         COMMENT 'The UNESCO country label',
  `hdi_label`                       string         COMMENT 'The Human Development Index country label',
  `affiliate_label`                 string         COMMENT 'The name associated with affiliates for the country or area',
  `world_bank_label`                string         COMMENT 'The World Bank country label',
  `m49_country_code`                string         COMMENT 'The ISO 3166-1 numeric country code',
  `un_continent_description`        string         COMMENT 'The UN continent description',
  `un_subcontinent_description`     string         COMMENT 'The UN subcontinent description',
  `imf_label`                       string         COMMENT 'The IMF country label',
  `insights_country_label`          string         COMMENT 'The Insights country label',
  `official_un_area_label`          string         COMMENT 'The official UN area label',
  `maxmind_label`                   string         COMMENT 'The MaxMind country label',
  `canonical_country_label`         string         COMMENT 'The canonical country label',
  `global_north_or_south`           string         COMMENT 'Denotes whether the country is north or south of the equator',
  `market_research_classification`  string         COMMENT 'The market research classification',
  `wikimedia_em_classification`     string         COMMENT 'The Wikimedia Emerging Market classification of the country',
  `nine_ninety_region_class`        string         COMMENT 'The 990 region classification of the country',
  `community_resource_class`        string         COMMENT 'The community resource classification of the country',
  `standard_un_area`                string         COMMENT 'Yes/False if the country is a standard UN area',
  `wmf_region_class`                string         COMMENT 'The Wikimedia Foundation region classification of the country',
  `standard_world_bank_area`        string         COMMENT 'Yes/False if the country is a standard World Bank area',
  `standard_imf_area`               string         COMMENT 'Yes/False if the country is a standard IMF area',
  `standard_pwt_area`               string         COMMENT 'Yes/False if the country is a standard Penn World Table area'
  )
COMMENT
  'Contains country metadata'
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/country_meta_data'
;