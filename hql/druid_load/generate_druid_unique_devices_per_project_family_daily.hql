-- Extracts one day of formatted daily unique devices
-- per-project-family to be loaded in Druid
--
-- Usage:
--     spark-sql -f generate_druid_unique_devices_per_project_family_daily.hql \
--         -d source_table=wmf_readership.unique_devices_per_project_family_daily \
--         -d destination_table=tmp_druid_unique_devices_per_project_family_daily_2023_04_01 \
--         -d destination_directory=/wmf/tmp/druid/unique_devices_per_project_family_daily_json \
--         -d day=2023-04-01
--


DROP TABLE IF EXISTS ${destination_table};


CREATE TABLE IF NOT EXISTS ${destination_table} (
    `dt`                     string,
    `project_family`          string,
    `country`                string,
    `country_code`           string,
    `uniques_underestimate`  bigint,
    `uniques_offset`         bigint,
    `uniques_estimate`       bigint
)
USING PARQUET
OPTIONS ('compression'='gzip')
LOCATION '${destination_directory}';


INSERT OVERWRITE TABLE ${destination_table}
SELECT /*+ COALESCE(1) */
    CONCAT('${day}', 'T00:00:00Z') AS dt,
    project_family AS project_family,
    country AS country,
    country_code AS country_code,
    uniques_underestimate AS uniques_underestimate,
    uniques_offset AS uniques_offset,
    uniques_estimate AS uniques_estimate
FROM ${source_table}
WHERE day = TO_DATE('${day}', 'yyyy-MM-dd');
