CREATE EXTERNAL TABLE IF NOT EXISTS gdi.country_meta_data (
`canonical_country_name`        STRING          COMMENT 'The country label aligned to canonical "countries" table',
`country_code_iso_3`            STRING          COMMENT 'ISO 3166 3-alpha country code',
`country_code_iso_2`            STRING          COMMENT 'ISO 3166 2-alpha country code (aligned to country code of canonical.countries table)',
`country_code_m49`              STRING          COMMENT 'United Nations m49 numeric location code',
`continent_name`                STRING          COMMENT 'United Nations continent classification with the addition of Heard Island and McDonald Islands, Kosovo, and Taiwan',
`subcontinent_name`             STRING          COMMENT 'United Nations subcontinent classification with the addition of Heard Island and McDonald Islands, Kosovo, and Taiwan',
`wmf_region`                    STRING          COMMENT 'The current Wikimedia Foundation regional classification',
`grant_committee_region`        STRING          COMMENT 'The regional classification of countries to regional Grantmaking committees',
`nine_ninety_region`            STRING          COMMENT 'The regional classification for the annual 990 financial report which is publicly shared by the Foundation',
`emerging_classification`       STRING          COMMENT 'The 2017 Community Resources country classification of developed, emerging, and least developed Wikimedia communities.',
`wmf_market_research`           STRING          COMMENT '2020 Foundation Market Research classifications',
`is_imf_country`                STRING          COMMENT 'Binary indicator as to whether the country is a standard location among International Monetary Fund nations',
`is_pwt_country`                STRING          COMMENT 'Binary indicator as to whether the country is a standard location among Penn World Table nations',
`is_un_country`                 STRING          COMMENT 'Binary indicator as to whether the country is a standard location among United Nations nations',
`is_world_bank_country`         STRING          COMMENT 'Binary indicator as to whether the country is a standard location among World Bank nations'
)
COMMENT
  'Contains country metadata'
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/country_meta_data'
;