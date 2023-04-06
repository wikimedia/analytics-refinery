-- Extracts one day of json formatted hourly pageviews to be loaded in Druid
--
-- Usage:
--     hive -f generate_daily_druid_pageviews.hql \
--         -d source_table=wmf.pageview_hourly \
--         -d wiki_map_table=canonical_data.wikis \
--         -d namespace_map_table=wmf_raw.mediawiki_project_namespace_map \
--         -d destination_directory=/wmf/tmp/druid/daily_json_pageviews \
--         -d mediawiki_snapshot=2020-01 \
--         -d year=2020 \
--         -d month=3 \
--         -d day=10
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS tmp_daily_druid_pageviews_${year}_${month}_${day};


CREATE EXTERNAL TABLE IF NOT EXISTS tmp_daily_druid_pageviews_${year}_${month}_${day} (
    `dt`                        string,
    `project`                   string,
    `language_variant`          string,
    `project_family`            string,
    `namespace_is_content`      string,
    `namespace_is_talk`         string,
    `namespace_canonical_name`  string,
    `access_method`             string,
    `agent_type`                string,
    `referer_class`             string,
    `continent`                 string,
    `country_code`              string,
    `country`                   string,
    `subdivision`               string,
    `city`                      string,
    `ua_device_family`          string,
    `ua_browser_family`         string,
    `ua_browser_major`          string,
    `ua_os_family`              string,
    `ua_os_major`               string,
    `ua_os_minor`               string,
    `ua_wmf_app_version`        string,
    `referer_name`              string,
    `view_count`                bigint
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';

WITH formatted_data AS (
    SELECT
        CONCAT(
            LPAD(year, 4, '0'), '-',
            LPAD(month, 2, '0'), '-',
            LPAD(day, 2, '0'), 'T',
            LPAD(hour, 2, '0'), ':00:00Z'
        ) AS dt,
        project,
        language_variant,
        wmt.database_group AS project_family,
        CAST(nmt.namespace_is_content AS BOOLEAN) AS namespace_is_content,
        IF(namespace_id IS NULL, NULL, namespace_id % 2 = 1) AS namespace_is_talk,
        IF(nmt.namespace_canonical_name = '', 'Main', nmt.namespace_canonical_name) AS namespace_canonical_name,
        access_method,
        agent_type,
        referer_class,
        continent,
        country_code,
        country,
        subdivision,
        city,
        user_agent_map['device_family'] AS ua_device_family,
        user_agent_map['browser_family'] AS ua_browser_family,
        user_agent_map['browser_major'] AS ua_browser_major,
        user_agent_map['os_family'] AS ua_os_family,
        user_agent_map['os_major'] AS ua_os_major,
        user_agent_map['os_minor'] AS ua_os_minor,
        user_agent_map['wmf_app_version'] AS ua_wmf_app_version,
        referer_name,
        view_count
    FROM ${source_table}
    LEFT JOIN ${wiki_map_table} AS wmt ON
        wmt.domain_name = IF(
            project IN ('wikidata', 'mediawiki'),
            CONCAT('www.', project, '.org'),
            CONCAT(project, '.org')
        )
    LEFT JOIN ${namespace_map_table} AS nmt ON
        nmt.hostname = IF(
            project IN ('wikidata', 'mediawiki'),
            CONCAT('www.', project, '.org'),
            CONCAT(project, '.org')
        ) AND
        nmt.namespace = namespace_id AND
        nmt.snapshot = '${mediawiki_snapshot}'
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day}
)

INSERT OVERWRITE TABLE tmp_daily_druid_pageviews_${year}_${month}_${day}
SELECT
    dt,
    project,
    language_variant,
    project_family,
    namespace_is_content,
    namespace_is_talk,
    namespace_canonical_name,
    access_method,
    agent_type,
    referer_class,
    continent,
    country_code,
    country,
    subdivision,
    city,
    ua_device_family,
    ua_browser_family,
    ua_browser_major,
    ua_os_family,
    ua_os_major,
    ua_os_minor,
    ua_wmf_app_version,
    referer_name,
    SUM(view_count) AS view_count
FROM formatted_data
GROUP BY
    dt,
    project,
    language_variant,
    project_family,
    namespace_is_content,
    namespace_is_talk,
    namespace_canonical_name,
    access_method,
    agent_type,
    referer_class,
    continent,
    country_code,
    country,
    subdivision,
    city,
    ua_device_family,
    ua_browser_family,
    ua_browser_major,
    ua_os_family,
    ua_os_major,
    ua_os_minor,
    ua_wmf_app_version,
    referer_name
;

DROP TABLE IF EXISTS tmp_daily_druid_pageviews_${year}_${month}_${day};
