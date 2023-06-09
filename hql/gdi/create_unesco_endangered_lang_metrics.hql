CREATE EXTERNAL TABLE IF NOT EXISTS gdi.`unesco_endangered_lang_metrics` (
  `language_name_english`           string          COMMENT 'Language name in English',
  `language_name_french`            string          COMMENT 'Language name in French',
  `language_name_spanish`           string          COMMENT 'Language name in Spanish',
  `countries`                       array<string>   COMMENT 'Countries where language is spoken',
  `continent`                       string          COMMENT 'Continent where country is located',
  `country_codes`                   array<string>   COMMENT 'Country codes where language is spoken',
  `language_codes`                  array<string>   COMMENT 'Language codes B639-3',
  `number_of_speakers`              int             COMMENT 'Number of speakers for the language for the partition year',
  `degree_of_endangerment`          string          COMMENT 'Degree of endangerment for the language for the partition year',
  `threat_level`                    int             COMMENT 'Threat level in terms of the endangerment for the partition year',
  `langauge_id`                     int             COMMENT 'Language id',
  `listed_country`                  string          COMMENT 'Listed country for the language',
  `combined_notes`                  string          COMMENT 'Combines multiple columns to create a descriptive string',
  `wikipedia_language_code`         string          COMMENT 'Wikipedia language code',
  `un_continent`                    string          COMMENT 'UN continent',
  `un_sub_continent`                string          COMMENT 'UN Sub-continent',
  `country_name`                    string          COMMENT 'Country name'
  )
COMMENT  'Contains languages that are endangered'
PARTITIONED BY (
  `year` int COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/unesco_endangered_lang_metrics'
;