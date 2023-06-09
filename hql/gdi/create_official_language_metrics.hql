CREATE EXTERNAL TABLE IF NOT EXISTS ntsako.`official_language_metrics` (
  `language_name`                   string   COMMENT 'Language name',
  `language_code_1`                 string   COMMENT 'Language code B639-1',
  `language_code_2`                 string   COMMENT 'Language code B639-2',
  `language_code_3`                 string   COMMENT 'Language code B639-3',
  `unesco_language_code`            int      COMMENT 'Unesco language code',
  `country_name`                    string   COMMENT 'Country name where language is spoken',
  `country_code`                    string   COMMENT 'Country code where the language is spoken',
  `region`                          string   COMMENT 'Region of the country where the language is spoken',
  `language_status`                 string   COMMENT 'Status of the language (Official/Vulnerable etc)',
  `un_continent`                    string   COMMENT 'Continent where the country of the language is spoken',
  `iban3_country_code`              string   COMMENT 'IBAN 3-letter country code',
  `iban2_country_code`              string   COMMENT 'IBAN 2-letter country code',
  `global_speakers`                 string   COMMENT 'Global speakers of the language',
  `language_notes`                  string   COMMENT 'Notes on the langauge',
  `un_sub_continent`                string   COMMENT 'UN Sub-continent'
  )
COMMENT  'Contains official language metrics'
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/user/ntsako/gdi/official_language_metrics'
;