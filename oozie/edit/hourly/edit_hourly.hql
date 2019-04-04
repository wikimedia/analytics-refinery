-- Parameters:
--     source_table   Fully qualified table name to extract data from.
--     destination_table   Fully qualified table name to write data to.
--     project_map_table   Fully qualified table name to join and get project.
--     snapshot   Snapshot to be computed in 'YYYY-MM' format.
--
-- Usage:
--     hive -f edit_hourly.hql \
--         -d source_table='wmf.mediawiki_history' \
--         -d destination_table='wmf.edit_hourly' \
--         -d project_map_table='wmf_raw.mediawiki_project_namespace_map' \
--         -d snapshot='2019-01' \
--

SET parquet.compression = SNAPPY;
SET mapred.reduce.tasks = 8;
SET hive.auto.convert.join = true;

WITH ungrouped_edit_hourly AS (
    SELECT
        FROM_UNIXTIME(
            UNIX_TIMESTAMP(event_timestamp, 'yyyy-MM-dd HH:mm:ss.sss'),
            'yyyy-MM-dd HH:00:00.0'
        ) AS ts,
        wiki_db,
        event_user_is_anonymous AS user_is_anonymous,
        ARRAY_CONTAINS(event_user_groups_historical, 'bot') AS user_is_bot,
        ARRAY_CONTAINS(event_user_groups_historical, 'sysop') AS user_is_administrator,
        event_user_groups_historical AS user_groups,
        page_namespace_is_content_historical AS namespace_is_content,
        page_namespace_historical % 2 == 1 AS namespace_is_talk,
        CASE page_namespace_historical
            WHEN 0 THEN 'Content'
            WHEN 1 THEN 'Talk'
            WHEN 2 THEN 'User'
            WHEN 3 THEN 'User talk'
            WHEN 4 THEN 'Project'
            WHEN 5 THEN 'Project talk'
            WHEN 6 THEN 'File'
            WHEN 7 THEN 'File talk'
            WHEN 8 THEN 'MediaWiki'
            WHEN 9 THEN 'MediaWiki talk'
            WHEN 10 THEN 'Template'
            WHEN 11 THEN 'Template talk'
            WHEN 12 THEN 'Help'
            WHEN 13 THEN 'Help talk'
            WHEN 14 THEN 'Category'
            WHEN 15 THEN 'Category talk'
            ELSE 'Other'
            END AS namespace_name,
        page_namespace_historical AS namespace_id,
        revision_parent_id == 0 AS creates_new_page,
        revision_is_deleted AS is_deleted,
        revision_is_identity_reverted AS is_reverted,
        CASE
            WHEN event_user_revision_count < 5 THEN '1-4'
            WHEN event_user_revision_count >= 5 AND event_user_revision_count < 100 THEN '5-99'
            WHEN event_user_revision_count >= 100 AND event_user_revision_count < 1000 THEN '100-999'
            WHEN event_user_revision_count >= 1000 AND event_user_revision_count < 10000 THEN '1000-9999'
            ELSE '10000+'
            END AS user_edit_count_bucket,
        revision_text_bytes_diff AS text_bytes_diff
    FROM
        ${source_table}
    WHERE
        snapshot = '${snapshot}' AND
        event_entity = 'revision' AND
        event_type = 'create' AND
        event_timestamp IS NOT NULL
)
INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(snapshot='${snapshot}')
    SELECT
        ts,
        -- Removing the .org suffix from hostname.
        SUBSTR(m.hostname, 0, LENGTH(m.hostname) - 4) AS project,
        user_is_anonymous,
        user_is_bot,
        user_is_administrator,
        user_groups,
        e.namespace_is_content,
        namespace_is_talk,
        namespace_name,
        namespace_id,
        creates_new_page,
        is_deleted,
        is_reverted,
        user_edit_count_bucket,
        COUNT(*) AS edit_count,
        SUM(text_bytes_diff) AS text_bytes_diff
    FROM ungrouped_edit_hourly AS e
    LEFT OUTER JOIN ${project_map_table} AS m ON
        m.snapshot = '${snapshot}' AND
        m.namespace = 0 AND
        e.wiki_db = m.dbname
    GROUP BY
        ts,
        SUBSTR(m.hostname, 0, LENGTH(m.hostname) - 4),
        user_is_anonymous,
        user_is_bot,
        user_is_administrator,
        user_groups,
        e.namespace_is_content,
        namespace_is_talk,
        namespace_name,
        namespace_id,
        creates_new_page,
        is_deleted,
        is_reverted,
        user_edit_count_bucket
;
