-- Generate a temporary geoeditors monthly dataset to be later ingested by Druid
--
-- Notes:
--      - Booleans are converted to 0/1 integers to allow using them both as dimensions and metrics in druid (having
--        them as metrics means for instance counting number of deleted pages)
--      - The `druid_` part in the name of the file is for disambiguation with the table in the wmf database.
--      - The table is dropped from the Airflow Druid ingestion job.
--
-- Usage:
--     spark3-sql \
--         -f generate_druid_geoeditors_monthly.hql \
--         -d source_table=wmf.geoeditors_monthly \
--         -d destination_table=tmp_druid_unique_devices_per_domain_daily_202303 \
--         -d destination_directory=hdfs:///wmf/tmp/druid/druid_load_geoeditors_monthly/202303 \
--         -d month=2023-03
--

DROP TABLE IF EXISTS ${destination_table};

CREATE TABLE IF NOT EXISTS ${destination_table} (
    `month`                           string COMMENT 'The partition of the data, needed for druid to have a timestamp',
    `wiki_db`                         string COMMENT 'The wiki database the editors worked in',
    `country_code`                    string COMMENT 'The 2-letter ISO country code this group of editors geolocated to, including Unknown (--)',
    `users_are_anonymous`             int    COMMENT 'Whether or not this group of editors edited anonymously',
    `activity_level`                  string COMMENT 'How many edits this group of editors performed, can be "1 to 4", "5 to 99", or "100 or more"',
    `distinct_editors`                bigint COMMENT 'Number of editors meeting this activity level',
    `namespace_zero_distinct_editors` bigint COMMENT 'Number of editors meeting this activity level with only namespace zero edits'
)
USING PARQUET
OPTIONS ('compression'='gzip')
LOCATION '${destination_directory}';

INSERT OVERWRITE TABLE ${destination_table}
SELECT /*+ COALESCE(1) */
    month,
    wiki_db,
    country_code,
    CASE WHEN users_are_anonymous THEN 1 ELSE 0 END AS users_are_anonymous,
    activity_level,
    distinct_editors,
    namespace_zero_distinct_editors
FROM ${source_table}
WHERE month = '${month}'
;
