-- Adds a snapshot partition of mediawiki history reduced data
--
-- REMARK: Booleans are converted to 0/1 integers to allow
-- using them both AS dimensions and metrics in druid (having
-- them AS metrics means for instance counting number of
-- deleted pages)
--
-- Usage:
--     hive -f generate_mediawiki_history_reduced.hql \
--         -d mw_denormalized_history_table=wmf.mediawiki_history \
--         -d mw_project_namespace_map_table=wmf_raw.mediawiki_project_namespace_map \
--         -d destination_table=wmf.mediawiki_history_reduced \
--         -d snapshot=2017-08
--

-- Bump memory for mappers and reducers
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;

WITH
    project_map AS (
        SELECT DISTINCT
            dbname AS wiki_db,
            -- Compatibility with exisitng AQS project hostnames
            REGEXP_REPLACE(hostname, '(www\\.|\\.org)', '') AS hostname
        FROM ${mw_project_namespace_map_table}
        WHERE TRUE
            AND snapshot = '${snapshot}'
    ),
    namespace_map AS (
        SELECT DISTINCT
            dbname AS wiki_db,
            namespace,
            IF (namespace_canonical_name != '', CONCAT(namespace_canonical_name, ':'), '') AS namespace_prefix
        FROM ${mw_project_namespace_map_table}
        WHERE TRUE
            AND snapshot = '${snapshot}'
    ),
    digest_base AS (
        SELECT
            pm.hostname AS project,
            CONCAT(SUBSTRING(event_timestamp, 0, 10), ' 00:00:00.0') AS event_timestamp_day,
            CONCAT(SUBSTRING(event_timestamp, 0, 7), '-01 00:00:00.0') AS event_timestamp_month,
            -- Coalesce null page_title to a value not to polute grouping-sets null (never seen, nullified in digests)
            CONCAT(COALESCE(nm.namespace_prefix, ''), COALESCE(page_title, page_title_historical, 'UNKNOWN_PAGE_TITLE')) AS page_title,
            -- Coalesce null user_text to a value not to polute grouping-sets null (never seen, nullified in digests)
            COALESCE(event_user_text, event_user_text_historical, 'UNKNOWN_USER_TEXT') AS user_text,
            IF (COALESCE(page_namespace_is_content, page_namespace_is_content_historical), 'content', 'non_content') AS page_type,
            page_is_redirect,
            CASE
                -- Using sequence to prevent writing NOT
                WHEN event_user_is_anonymous THEN 'anonymous'
                WHEN array_contains(COALESCE(event_user_is_bot_by, event_user_is_bot_by_historical), 'group') THEN 'group_bot'
                WHEN array_contains(COALESCE(event_user_is_bot_by, event_user_is_bot_by_historical), 'name') THEN 'name_bot'
                ELSE 'user'
            END AS user_type,
            revision_text_bytes_diff
        FROM ${mw_denormalized_history_table} mw
            INNER JOIN project_map pm
                ON (mw.wiki_db = pm.wiki_db)
            LEFT JOIN namespace_map nm
                ON (mw.wiki_db = nm.wiki_db
                    AND COALESCE(page_namespace, page_namespace_historical) = nm.namespace)
        WHERE TRUE
            AND snapshot = '${snapshot}'
            AND event_entity = 'revision'
            AND event_type = 'create'
            AND event_timestamp IS NOT NULL
            -- Explicitely remove deleted revisions from computation
            AND NOT revision_is_deleted_by_page_deletion
    ),

    user_digests AS (
        SELECT
            project,
            'user' AS event_entity,
            IF (event_timestamp_day IS NULL, 'monthly_digest', 'daily_digest') AS event_type,
            COALESCE(event_timestamp_day, event_timestamp_month) AS event_timestamp,
            NULL AS user_text,
            COALESCE(user_type, 'all') AS user_type,
            NULL AS page_title,
            NULL AS page_namespace,
            COALESCE(page_type, 'all') AS page_type,
            ARRAY() AS other_tags,
            SUM(revision_text_bytes_diff) AS text_bytes_diff,
            SUM(ABS(revision_text_bytes_diff)) AS text_bytes_diff_abs,
            COUNT(1) as revisions
        FROM digest_base
        GROUP BY
            project,
            event_timestamp_day,
            event_timestamp_month,
            user_text,
            user_type,
            page_type
        GROUPING SETS(
            (project, event_timestamp_day, user_text, user_type, page_type),
            (project, event_timestamp_day, user_text, user_type),
            (project, event_timestamp_day, user_text, page_type),
            (project, event_timestamp_day, user_text),
            (project, event_timestamp_month, user_text, user_type, page_type),
            (project, event_timestamp_month, user_text, user_type),
            (project, event_timestamp_month, user_text, page_type),
            (project, event_timestamp_month, user_text)
        )
    ),
    page_digests AS (
        SELECT
            project,
            'page' AS event_entity,
            IF (event_timestamp_day IS NULL, 'monthly_digest', 'daily_digest') AS event_type,
            COALESCE(event_timestamp_day, event_timestamp_month) AS event_timestamp,
            NULL AS user_text,
            COALESCE(user_type, 'all') AS user_type,
            NULL AS page_title,
            NULL AS page_namespace,
            COALESCE(page_type, 'all') AS page_type,
            ARRAY() AS other_tags,
            SUM(revision_text_bytes_diff) AS text_bytes_diff,
            SUM(ABS(revision_text_bytes_diff)) AS text_bytes_diff_abs,
            COUNT(1) as revisions
        FROM digest_base
        WHERE NOT page_is_redirect
        GROUP BY
            project,
            event_timestamp_day,
            event_timestamp_month,
            page_title,
            user_type,
            page_type
        GROUPING SETS(
            (project, event_timestamp_day, page_title, user_type, page_type),
            (project, event_timestamp_day, page_title, user_type),
            (project, event_timestamp_day, page_title, page_type),
            (project, event_timestamp_day, page_title),
            (project, event_timestamp_month, page_title, user_type, page_type),
            (project, event_timestamp_month, page_title, user_type),
            (project, event_timestamp_month, page_title, page_type),
            (project, event_timestamp_month, page_title)
        )
    ),

    core_data AS (
        SELECT
            pm.hostname AS project,
            event_entity,
            event_type,
            CASE
              WHEN event_entity = 'page' AND event_type = 'create' THEN COALESCE(page_first_edit_timestamp, event_timestamp)
              ELSE event_timestamp
            END AS event_timestamp,
            COALESCE(event_user_text, event_user_text_historical) AS user_text,
            CASE
                -- Using sequence to prevent writing NOT
                WHEN event_user_is_anonymous THEN 'anonymous'
                WHEN array_contains(COALESCE(event_user_is_bot_by, event_user_is_bot_by_historical), 'group') THEN 'group_bot'
                WHEN array_contains(COALESCE(event_user_is_bot_by, event_user_is_bot_by_historical), 'name') THEN 'name_bot'
                ELSE 'user'
            END AS user_type,
            CONCAT(COALESCE(nm.namespace_prefix, ''), COALESCE(page_title, page_title_historical)) AS page_title,
            page_namespace,
            CASE
                WHEN COALESCE(page_namespace_is_content, page_namespace_is_content_historical) THEN 'content'
                ELSE 'non_content'
            END AS page_type,
            -- Trick to get an array of values without nulls
            SPLIT(CONCAT_WS('|',
                    IF (page_is_redirect, 'redirect', NULL),
                    IF (user_is_created_by_self, 'self_created', NULL)
            ), '\\|') AS other_tags,
            revision_text_bytes_diff AS text_bytes_diff,
            ABS(revision_text_bytes_diff) AS text_bytes_diff_abs,
            IF (event_entity = 'revision', 1, 0) AS revisions
        FROM ${mw_denormalized_history_table} mw
            INNER JOIN project_map pm
                ON (mw.wiki_db = pm.wiki_db)
            LEFT JOIN namespace_map nm
                ON (mw.wiki_db = nm.wiki_db
                    AND COALESCE(page_namespace, page_namespace_historical) = nm.namespace)
        WHERE TRUE
            AND snapshot = '${snapshot}'
            -- Only export rows with valid timestamp (page_first_edit_timestamp for page-create, event_timestamp otherwise)
            AND ((event_entity = 'page' AND event_type = 'create' AND COALESCE(page_first_edit_timestamp, event_timestamp) IS NOT NULL)
                OR  (event_timestamp IS NOT NULL))
            -- Explicitly remove deleted events
            AND (NOT event_entity = 'page' OR NOT page_is_deleted)
            AND (NOT event_entity = 'revision' OR NOT revision_is_deleted_by_page_deletion)
    )

INSERT OVERWRITE TABLE ${destination_table} partition (snapshot='${snapshot}')

SELECT * FROM core_data
  UNION ALL
SELECT * FROM user_digests
  UNION ALL
SELECT * FROM page_digests
;
