-- Extracts one month of json formatted hourly virtualpageviews to be loaded in Druid
--
-- Usage:
--     hive -f generate_monthly_druid_virtualpageviews.hql \
--         -d source_table=wmf.virtualpageview_hourly \
--         -d destination_directory=/wmf/tmp/druid/monthly_json_virtualpageviews \
--         -d year=2018 \
--         -d month=3
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;

DROP TABLE IF EXISTS tmp_monthly_druid_virtualpageviews_${year}_${month};

CREATE EXTERNAL TABLE IF NOT EXISTS tmp_monthly_druid_virtualpageviews_${year}_${month} (
    `dt`                  string,
    `project`             string,
    `language_variant`    string,
    `access_method`       string,
    `agent_type`          string,
    `referer_class`       string,
    `continent`           string,
    `country_code`        string,
    `country`             string,
    `ua_browser_family`   string,
    `ua_browser_major`    string,
    `ua_os_family`        string,
    `ua_os_major`         string,
    `ua_os_minor`         string,
    `ua_wmf_app_version`  string,
    `view_count`          bigint
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';

INSERT OVERWRITE TABLE tmp_monthly_druid_virtualpageviews_${year}_${month}
SELECT
    CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-',
        LPAD(day, 2, '0'), 'T',
        LPAD(hour, 2, '0'), ':00:00Z') AS dt,
    project AS project,
    language_variant AS language_variant,
    access_method AS access_method,
    agent_type AS agent_type,
    referer_class AS referer_class,
    continent AS continent,
    country_code AS country_code,
    country AS country,
    user_agent_map['browser_family'] AS ua_browser_family,
    user_agent_map['browser_major'] AS ua_browser_major,
    user_agent_map['os_family'] AS ua_os_family,
    user_agent_map['os_major'] AS ua_os_major,
    user_agent_map['os_minor'] AS ua_os_minor,
    user_agent_map['wmf_app_version'] AS ua_wmf_app_version,
    SUM(view_count) AS view_count
FROM ${source_table}
WHERE year = ${year}
    AND month = ${month}
GROUP BY
    CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-',
        LPAD(day, 2, '0'), 'T',
        LPAD(hour, 2, '0'), ':00:00Z'),
    project,
    language_variant,
    access_method,
    agent_type,
    referer_class,
    continent,
    country_code,
    country,
    user_agent_map['browser_family'],
    user_agent_map['browser_major'],
    user_agent_map['os_family'],
    user_agent_map['os_major'],
    user_agent_map['os_minor'],
    user_agent_map['wmf_app_version']
;

DROP TABLE IF EXISTS tmp_monthly_druid_virtualpageviews_${year}_${month};
