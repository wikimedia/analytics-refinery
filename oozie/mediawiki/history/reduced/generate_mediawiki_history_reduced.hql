-- Adds a snapshot partition of mediawiki history reduced data
--
-- REMARK: Booleans are converted to 0/1 integers to allow
-- using them both AS dimensions and metrics in druid (having
-- them AS metrics means for instance counting number of
-- deleted pages)
--
-- Usage:
--     hive -f generate_mediawiki_history.hql \
--         -d mw_denormalized_history_table=wmf.mediawiki_history \
--         -d mw_project_namespace_map_table=wmf_raw.project_namespace_map \
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
            CONCAT(COALESCE(nm.namespace_prefix, ''), COALESCE(page_title, page_title_historical)) AS page_title,
            COALESCE(event_user_text, event_user_text_historical) AS user_text,
            IF (COALESCE(page_namespace_is_content, page_namespace_is_content_historical), 'content', 'non_content') AS page_type,
            page_is_redirect,
            CASE
                -- Using sequence to prevent writing NOT
                WHEN event_user_is_anonymous THEN 'anonymous'
                WHEN array_contains(event_user_groups, 'bot') THEN 'group_bot'
                WHEN COALESCE(event_user_text, event_user_text_historical) RLIKE '(?i)^.*bot([^a-z].*$|$)' THEN 'name_bot'
                ELSE 'user'
            END AS user_type,
            revision_deleted_timestamp,
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
            event_timestamp,
            COALESCE(event_user_text, event_user_text_historical) AS user_text,
            CASE
                -- Using sequence to prevent writing NOT
                WHEN event_user_is_anonymous THEN 'anonymous'
                WHEN array_contains(event_user_groups, 'bot') THEN 'group_bot'
                WHEN COALESCE(event_user_text, event_user_text_historical) RLIKE '(?i)^.*bot([^a-z].*$|$)' THEN 'name_bot'
                ELSE 'user'
            END AS user_type,
            CONCAT(COALESCE(nm.namespace_prefix, ''), COALESCE(page_title, page_title_historical)) AS page_title,
            page_namespace,
            CASE
                WHEN page_namespace_is_content THEN 'content'
                ELSE 'non_content'
            END AS page_type,
            -- Trick to get an array of values without nulls
            SPLIT(CONCAT_WS('|',
                    IF (unix_timestamp(event_timestamp) - unix_timestamp(event_user_creation_timestamp) <= 86400,
                        'user_first_24_hours', NULL),
                    IF (page_is_redirect, 'redirect', NULL),
                    IF (revision_is_deleted, 'deleted', NULL),
                    CASE
                        WHEN SUBSTRING(event_timestamp, 0, 10) = SUBSTRING(revision_deleted_timestamp, 0, 10) THEN 'deleted_day'
                        WHEN SUBSTRING(event_timestamp, 0, 7) = SUBSTRING(revision_deleted_timestamp, 0, 7) THEN 'deleted_month'
                        WHEN SUBSTRING(event_timestamp, 0, 4) = SUBSTRING(revision_deleted_timestamp, 0, 4) THEN 'deleted_year'
                        ELSE NULL
                    END,
                    IF (revision_is_identity_reverted, 'reverted', NULL),
                    -- Not needed as of now - Keeping for possible future
                    -- CASE
                    --     WHEN revision_seconds_to_identity_revert <= 60 THEN 'reverted_minute'
                    --     WHEN revision_seconds_to_identity_revert <= 5 * 60 THEN 'reverted_5_minutes'
                    --     WHEN revision_seconds_to_identity_revert <= 10 * 60 THEN 'reverted_10_minutes'
                    --     WHEN revision_seconds_to_identity_revert <= 30 * 60 THEN 'reverted_30_minutes'
                    --     WHEN revision_seconds_to_identity_revert <= 60 * 60 THEN 'reverted_hour'
                    --     WHEN revision_seconds_to_identity_revert <= 12 * 60 * 60 THEN 'reverted_12_hours'
                    --     WHEN revision_seconds_to_identity_revert <= 24 * 60 * 60 THEN 'reverted_day'
                    --     WHEN revision_seconds_to_identity_revert <= 3 * 24 * 60 * 60 THEN 'reverted_3_days'
                    --     WHEN revision_seconds_to_identity_revert <= 7 * 24 * 60 * 60 THEN 'reverted_week'
                    --     WHEN revision_seconds_to_identity_revert <= 2 * 7 * 24 * 60 * 60 THEN 'reverted_2_weeks'
                    --     WHEN revision_seconds_to_identity_revert <= 30 * 24 * 60 * 60 THEN 'reverted_month'
                    --     WHEN revision_seconds_to_identity_revert <= 3 * 30 * 24 * 60 * 60 THEN 'reverted_3_months'
                    --     WHEN revision_seconds_to_identity_revert <= 6 * 30 * 24 * 60 * 60 THEN 'reverted_6_months'
                    --     WHEN revision_seconds_to_identity_revert <= 365 * 24 * 60 * 60 THEN 'reverted_year'
                    --     ELSE NULL
                    --END,
                    IF (revision_is_identity_revert, 'revert', NULL),
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
            -- Only export rows with valid timestamp format
            AND event_timestamp IS NOT NULL
    )

INSERT OVERWRITE TABLE ${destination_table} partition (snapshot='${snapshot}')

SELECT * FROM core_data
  UNION ALL
SELECT * FROM user_digests
  UNION ALL
SELECT * FROM page_digests
;
