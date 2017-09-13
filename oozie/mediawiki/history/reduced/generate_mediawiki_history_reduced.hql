-- Generate json formatted mediawiki history to be loaded in Druid
--
-- REMARK: Booleans are converted to 0/1 integers to allow
-- using them both AS dimensions and metrics in druid (having
-- them AS metrics means for instance counting number of
-- deleted pages)
--
-- Usage:
--     hive -f generate_json_mediawiki_history.hql \
--         -d mw_denormalized_history_table=wmf.mediawiki_history \
--         -d mw_project_namespace_map_table=wmf_raw.project_namespace_map \
--         -d destination_directory=/tmp/druid/mediawiki_history \
--         -d snapshot=2017-03
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS `tmp_druid_mediawiki_history_reduced`;


CREATE EXTERNAL TABLE IF NOT EXISTS `tmp_druid_mediawiki_history_reduced` (
  `project`                                       string,
  `event_entity`                                  string,
  -- Needed for page and user create / delete
  `event_type`                                    string,
  `event_timestamp`                               string,
  `event_user_id`                                 bigint,
  `event_user_text`                               string,
  `event_user_is_anonymous`                       int,
  `event_user_is_bot_by_name`                     int,
  `event_user_is_bot_by_group`                    int,
  `event_user_creation_is_within_one_day`         int,
  `event_user_month_activity_level`               int,
  `page_id`                                       bigint,
  `page_title`                                    string,
  `page_namespace`                                int,
  `page_namespace_is_content`                     int,
  `page_is_redirect_latest`                       int,
  `page_month_activity_level`                     int,
  `revision_text_bytes_diff`                      bigint,
  `revision_text_bytes_diff_absolute`             bigint
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';


WITH
    event_user_month_activity_level AS (
        SELECT
            wiki_db,
            SUBSTRING(event_timestamp, 0, 7) AS event_month,
            event_user_id,
            COUNT(1) AS activity_level
        FROM ${mw_denormalized_history_table}
        WHERE TRUE
            AND snapshot = '${snapshot}'
            AND event_entity = 'revision'
            AND event_type = 'create'
            AND event_timestamp IS NOT NULL
            AND NOT event_user_is_anonymous
        GROUP BY
            wiki_db,
            SUBSTRING(event_timestamp, 0, 7),
            event_user_id
    ),

    page_month_activity_level AS (
        SELECT
            wiki_db,
            SUBSTRING(event_timestamp, 0, 7) AS event_month,
            page_id,
            COUNT(1) AS activity_level
        FROM ${mw_denormalized_history_table}
        WHERE TRUE
            AND snapshot = '${snapshot}'
            AND event_entity = 'revision'
            AND event_type = 'create'
            AND event_timestamp IS NOT NULL
            AND page_id IS NOT NULL
            AND page_id > 0
        GROUP BY
            wiki_db,
            SUBSTRING(event_timestamp, 0, 7),
            page_id
    ),

    project_map AS (
        SELECT DISTINCT
            dbname AS wiki_db,
            -- Compatibility with exisitng AQS project hostnames
            REGEXP_REPLACE(hostname, '(www\\.|\\.org)', '') AS hostname
        FROM ${mw_project_namespace_map_table}
        WHERE TRUE
            AND snapshot = '${snapshot}'
    )

INSERT OVERWRITE TABLE tmp_druid_mediawiki_history_reduced
SELECT
    pm.hostname,
    event_entity,
    event_type,
    event_timestamp,
    mw.event_user_id,
    COALESCE(event_user_text_latest, event_user_text) AS event_user_text,
    CASE WHEN event_user_is_anonymous THEN 1 ELSE 0 END AS event_user_is_anonymous,
    CASE WHEN array_contains(event_user_groups, 'bot') THEN 1 ELSE 0 END AS event_user_is_bot_by_group,
    CASE WHEN event_user_is_bot_by_name THEN 1 ELSE 0 END AS event_user_is_bot_by_name,
    CASE WHEN unix_timestamp(event_timestamp) - unix_timestamp(event_user_creation_timestamp) <= 86400 THEN 1 ELSE 0 END AS event_user_creation_is_within_one_day,
    ual.activity_level AS event_user_month_activity_level,
    mw.page_id,
    COALESCE(page_title_latest, page_title) AS page_title,
    page_namespace,
    CASE WHEN page_namespace_is_content THEN 1 ELSE 0 END AS page_namespace_is_content,
    CASE WHEN page_is_redirect_latest THEN 1 ELSE 0 END AS page_is_redirect_latest,
    pal.activity_level AS page_month_activity_level,
    revision_text_bytes_diff,
    ABS(revision_text_bytes_diff) AS revision_text_bytes_diff_absolute
FROM ${mw_denormalized_history_table} mw
    LEFT JOIN event_user_month_activity_level ual
        ON ((mw.wiki_db = ual.wiki_db)
            AND (SUBSTRING(mw.event_timestamp, 0, 7) = ual.event_month)
            AND (mw.event_user_id = ual.event_user_id))
    LEFT JOIN page_month_activity_level pal
        ON ((mw.wiki_db = pal.wiki_db)
            AND (SUBSTRING(mw.event_timestamp, 0, 7) = pal.event_month)
            AND (mw.page_id = pal.page_id))
    LEFT JOIN project_map pm
        ON (mw.wiki_db = pm.wiki_db)

WHERE TRUE
    AND snapshot = '${snapshot}'
    -- Only export rows with valid timestamp format
    AND event_timestamp IS NOT NULL
    AND ((event_entity = 'revision')
      -- keep page create, delete and restore for meASure and adjustments
      OR ((event_entity = 'page') AND (event_type != 'move'))
      -- keep user created by self for measure
      OR ((event_entity = 'user') AND (event_type == 'create') AND (user_is_created_by_self)))
;


DROP TABLE IF EXISTS tmp_druid_mediawiki_history_reduced;
