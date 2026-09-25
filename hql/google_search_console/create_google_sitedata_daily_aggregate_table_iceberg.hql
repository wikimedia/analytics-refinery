-- Create table statement for the Google Search Console sitedata daily aggregate Iceberg table.
--
-- Usage
--     spark3-sql -f create_google_sitedata_daily_aggregate_table_iceberg.hql \
--         --database wmf_readership \
--         -d location=hdfs://analytics-hadoop/wmf/data/wmf_readership/google_sitedata_daily_aggregate
--

use wmf_readership ;
CREATE EXTERNAL TABLE IF NOT EXISTS `google_sitedata_daily_aggregate` (
    `data_date`    date    COMMENT 'Day on which the data in this row was generated (Pacific Time); the table is partitioned by its year.',
    `site_url`     string  COMMENT 'URL of the property. Domain properties are sc-domain:property-name; URL-prefix properties are the full URL.',
    `country`      string  COMMENT 'Country of the query, ISO-3166-1-Alpha-3 code.',
    `search_type`  string  COMMENT 'Search surface: web, image, video, news, discover or googleNews.',
    `device`       string  COMMENT 'Device used for the query (DESKTOP, MOBILE, TABLET).',
    `impressions`  bigint  COMMENT 'Total impressions for this (date, site_url, country, search_type, device).',
    `clicks`       bigint  COMMENT 'Total clicks for this (date, site_url, country, search_type, device).',
    `date`         string  COMMENT 'Day of the aggregation (YYYY-MM-DD), matching data_date.'
)
USING ICEBERG
PARTITIONED BY (years(data_date))
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION 'hdfs://analytics-hadoop/wmf/data/wmf_readership/google_sitedata_daily_aggregate'
;
