-- Create table statement for geoeditors_public_monthly table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_geoeditors_public_monthly_table.hql   \
--          --database wmf

CREATE EXTERNAL TABLE `geoeditors_public_monthly` (
    `wiki_db`           string    COMMENT 'The wiki database this group of editors worked in',
    `project`           string    COMMENT 'The project the editors worked in',
    `country_name`      string    COMMENT 'The country this group of editors is geolocated to, including unknown',
    `country_code`      string    COMMENT 'The ISO 3166-1 alpha-2 country code of the country this group of editors is geolocated to, including unknown as --',
    `activity_level`    string    COMMENT 'How many edits this group of editors performed, can be "5 to 99", or "100 or more"',
    `editors_ceil`      bigint    COMMENT 'Editor count upper bound for this bucket. Lower bound is upper bound - 9.'
) PARTITIONED BY (
    `month`             string    COMMENT 'The month that the data applies to'
)
STORED AS PARQUET
LOCATION
    'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki_private/geoeditors_public_monthly'
;
