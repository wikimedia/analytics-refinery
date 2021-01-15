-- Extracts one day of json formatted daily unique devices
-- per-project-family to be loaded in Druid
--
-- Usage:
--     hive -f generate_druid_unique_devices_per_project_family_daily.hql \
--         -d source_table=wmf.unique_devices_per_project_family_daily \
--         -d destination_directory=/wmf/tmp/druid/unique_devices_per_project_family_daily_json \
--         -d year=2017 \
--         -d month=4 \
--         -d day=1
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS tmp_druid_unique_devices_per_project_family_daily_${year}_${month}_${day};


CREATE EXTERNAL TABLE IF NOT EXISTS tmp_druid_unique_devices_per_project_family_daily_${year}_${month}_${day} (
    `dt`                     string,
    `project_family`          string,
    `country`                string,
    `country_code`           string,
    `uniques_underestimate`  bigint,
    `uniques_offset`         bigint,
    `uniques_estimate`       bigint
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';


INSERT OVERWRITE TABLE tmp_druid_unique_devices_per_project_family_daily_${year}_${month}_${day}
SELECT
    CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-',
        LPAD(day, 2, '0'), 'T00:00:00Z') AS dt,
    project_family AS project_family,
    country AS country,
    country_code AS country_code,
    uniques_underestimate AS uniques_underestimate,
    uniques_offset AS uniques_offset,
    uniques_estimate AS uniques_estimate
FROM ${source_table}
WHERE year = ${year}
    AND month = ${month}
    AND day = ${day}
    AND project_family != 'wikimedia';


DROP TABLE IF EXISTS tmp_druid_unique_devices_per_project_family_daily_${year}_${month}_${day};
