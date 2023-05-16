-- Extracts one month of formatted daily pageviews to be loaded in Druid
--
-- Usage:
--     spark3-sql --master yarn -f generate_monthly_druid_pageviews.hql \
--         -d source_table=wmf.pageview_hourly \
--         -d wiki_map_table=canonical_data.wikis \
--         -d namespace_map_table=wmf_raw.mediawiki_project_namespace_map \
--         -d destination_directory=/wmf/tmp/druid/monthly_json_pageviews \
--         -d destination_table=tmp_monthly_druid_pageviews_2020_02 \
--         -d mediawiki_snapshot=2020-01 \
--         -d year=2020 \
--         -d month=2 \
--         -d coalesce_partitions=16
--

DROP TABLE IF EXISTS ${destination_table};


CREATE TABLE IF NOT EXISTS ${destination_table} (
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
    `ua_browser_family`         string,
    `ua_browser_major`          string,
    `ua_os_family`              string,
    `ua_os_major`               string,
    `ua_os_minor`               string,
    `ua_wmf_app_version`        string,
    `view_count`                bigint
)
USING PARQUET
OPTIONS ('compression'='gzip')
LOCATION '${destination_directory}';

WITH formatted_data AS (
    SELECT /*+ broadcast(nmt) */
        dt,
        project,
        language_variant,
        project_family,
        CAST(nmt.namespace_is_content AS BOOLEAN) AS namespace_is_content,
        namespace_is_talk,
        IF(nmt.namespace_canonical_name = '', 'Main', nmt.namespace_canonical_name) AS namespace_canonical_name,
        access_method,
        agent_type,
        referer_class,
        continent,
        country_code,
        country,
        ua_browser_family,
        ua_browser_major,
        ua_os_family,
        ua_os_major,
        ua_os_minor,
        ua_wmf_app_version,
        view_count
    FROM (
        SELECT /*+ broadcast(wmt) */
            CONCAT(
                LPAD(year, 4, '0'), '-',
                LPAD(month, 2, '0'), '-',
                LPAD(day, 2, '0'), 'T00:00:00Z'
            ) AS dt,
            project,
            language_variant,
            wmt.database_group AS project_family,
            IF(namespace_id IS NULL, NULL, namespace_id % 2 = 1) AS namespace_is_talk,
            access_method,
            agent_type,
            referer_class,
            continent,
            country_code,
            country,
            user_agent_map['browser_family'] AS ua_browser_family,
            user_agent_map['browser_major'] AS ua_browser_major,
            user_agent_map['os_family'] AS ua_os_family,
            user_agent_map['os_major'] AS ua_os_major,
            user_agent_map['os_minor'] AS ua_os_minor,
            user_agent_map['wmf_app_version'] AS ua_wmf_app_version,
            view_count,
            namespace_id
        FROM ${source_table}
        LEFT JOIN ${wiki_map_table} AS wmt ON
            wmt.domain_name = IF(
                project IN ('wikidata', 'mediawiki'),
                CONCAT('www.', project, '.org'),
                CONCAT(project, '.org')
            )
        WHERE
            year = ${year} AND
            month = ${month}
    ) AS pageview_wikimap
    LEFT JOIN ${namespace_map_table} AS nmt ON
        nmt.hostname = IF(
            project IN ('wikidata', 'mediawiki'),
            CONCAT('www.', project, '.org'),
            CONCAT(project, '.org')
        ) AND
        nmt.namespace = namespace_id AND
        nmt.snapshot = '${mediawiki_snapshot}'
)

INSERT OVERWRITE TABLE ${destination_table}
SELECT /*+COALESCE(${coalesce_partitions})*/
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
    ua_browser_family,
    ua_browser_major,
    ua_os_family,
    ua_os_major,
    ua_os_minor,
    ua_wmf_app_version,
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
    ua_browser_family,
    ua_browser_major,
    ua_os_family,
    ua_os_major,
    ua_os_minor,
    ua_wmf_app_version
;
