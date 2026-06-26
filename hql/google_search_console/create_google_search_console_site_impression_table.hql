-- Creates the raw table for the Google Search Console
-- "searchdata_site_impression" data export (data aggregated by property).
--
-- Usage
--     hive -f create_google_search_console_site_impression_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE IF NOT EXISTS `google_search_console_site_impression` (
    `data_date`             date    COMMENT 'Day on which the data in this row was generated (Pacific Time). Same value as the date partition.',
    `site_url`              string  COMMENT 'URL of the property. Domain properties are sc-domain:property-name; URL-prefix properties are the full URL.',
    `query`                 string  COMMENT 'The user query. Empty string when is_anonymized_query is true.',
    `is_anonymized_query`   boolean COMMENT 'True for rare (anonymized) queries; query is then an empty string.',
    `country`               string  COMMENT 'Country of the query, ISO-3166-1-Alpha-3 code.',
    `search_type`           string  COMMENT 'Search surface: web, image, video, news, discover or googleNews.',
    `device`                string  COMMENT 'Device used for the query (DESKTOP, MOBILE, TABLET).',
    `impressions`           bigint,
    `clicks`                bigint,
    `sum_top_position`      bigint  COMMENT 'Sum of the topmost positions; average position = sum_top_position / impressions + 1.'
)
COMMENT
    'Google Search Console data export, aggregated by property.'
PARTITIONED BY (
    `date`      string COMMENT 'Day of the export partition (YYYY-MM-DD), matching data_date.',
    `domain`    string COMMENT 'Wiki family the property belongs to: wikipedia or wikimedia.'
)
STORED AS PARQUETFILE
LOCATION '/wmf/data/raw/google_search_console/searchdata_site_impression'
TBLPROPERTIES ('parquet.compression'='GZIP')
;
