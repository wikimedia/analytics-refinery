-- Parameters:
--     source_table   Fully qualified table name to extract data from.
--     destination_table   Fully qualified table name to write data to.
--     wiki_map_table      Fully qualified table name to join and get wiki.
--     snapshot   Snapshot to be computed in 'YYYY-MM' format.
--
-- Usage:
--     hive -f edit_hourly.hql \
--         -d source_table='wmf.mediawiki_history' \
--         -d destination_table='wmf.edit_hourly' \
--         -d wiki_map_table='canonical_data.wikis' \
--         -d snapshot='2019-01'
--

SET parquet.compression = SNAPPY;
SET mapred.reduce.tasks = 8;

-- Note about the following parameters:
-- "hive.auto.convert.join" is currently set to false in hive-site.xml
-- but used only for special cases like this script (that
-- currently takes only one hour to complete and several hours without it).
-- In hive actions the client communicates directly with the Hive Metastore
-- and HDFS, so the local optimizations are executed on the client itself.
-- In hive2 actions the local optimizations are done in the Hive Server2's local
-- environment, by default spawning a new JVM (for safety). The main problem with
-- this is that any HADOOP_OPTS that holds state (like Prometheus' jmx javaagent
-- that binds a network port) will be inherited by the new JVM that will fail
-- (in the Prometheus example due to the port already used/bound).
--
-- "hive.exec.submit.local.task.via.child = false" allows us to use hive2 actions,
-- since the local optimizations are done in the Hive Server 2's JVM, without
-- the risk of executing again HADOOP_OPTS.
SET hive.auto.convert.join = true;
SET hive.exec.submit.local.task.via.child = false;

WITH edit_history AS (
    SELECT
        event_timestamp,
        wiki_db,
        event_user_is_anonymous,
        event_user_groups_historical,
        event_user_is_bot_by_historical,
        UNIX_TIMESTAMP(event_timestamp, 'yyyy-MM-dd HH:mm:ss.sss') - UNIX_TIMESTAMP(
            LEAST(
                COALESCE(event_user_registration_timestamp, event_user_creation_timestamp, event_user_first_edit_timestamp),
                COALESCE(event_user_creation_timestamp, event_user_first_edit_timestamp, event_user_registration_timestamp),
                COALESCE(event_user_first_edit_timestamp, event_user_registration_timestamp, event_user_creation_timestamp)
            ),
            'yyyy-MM-dd HH:mm:ss.sss'
        ) AS user_tenure,
        page_namespace_is_content_historical,
        page_namespace_historical,
        page_is_redirect,
        revision_parent_id,
        revision_is_deleted_by_page_deletion,
        revision_is_identity_reverted,
        event_user_revision_count,
        revision_tags,
        revision_text_bytes_diff
    FROM ${source_table}
    WHERE
        snapshot = '${snapshot}' AND
        event_entity = 'revision' AND
        event_type = 'create' AND
        event_timestamp IS NOT NULL
),

formatted_edit_history AS (
    SELECT
        FROM_UNIXTIME(
            UNIX_TIMESTAMP(event_timestamp, 'yyyy-MM-dd HH:mm:ss.sss'),
            'yyyy-MM-dd HH:00:00.0'
        ) AS ts,
        wiki_db,
        event_user_is_anonymous AS user_is_anonymous,
        SIZE(event_user_is_bot_by_historical) > 0 AS user_is_bot,
        ARRAY_CONTAINS(event_user_groups_historical, 'sysop') AS user_is_administrator,
        event_user_groups_historical AS user_groups,
        CASE
            WHEN user_tenure < 86400 THEN 'Under 1 day'
            WHEN user_tenure >= 86400 AND user_tenure < 7*86400 THEN '1 to 7 days'
            WHEN user_tenure >= 7*86400 AND user_tenure < 30*86400 THEN '7 to 30 days'
            WHEN user_tenure >= 30*86400 AND user_tenure < 90*86400 THEN '30 to 90 days'
            WHEN user_tenure >= 90*86400 AND user_tenure < 365*86400 THEN '90 days to 1 year'
            WHEN user_tenure >= 365*86400 AND user_tenure < 1095*86400 THEN '1 to 3 years'
            WHEN user_tenure >= 1095*86400 AND user_tenure < 3650*86400 THEN '3 to 10 years'
            WHEN user_tenure >= 3650*86400 THEN 'Over 10 years'
            ELSE 'Undefined'
        END AS user_tenure_bucket,
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
            WHEN 122 THEN 'Property'
            WHEN 123 THEN 'Property talk'
            ELSE 'Other'
        END AS namespace_name,
        page_namespace_historical AS namespace_id,
        revision_parent_id == 0 AS creates_new_page,
        revision_is_deleted_by_page_deletion AS is_deleted,
        revision_is_identity_reverted AS is_reverted,
        page_is_redirect AS is_redirect_currently,
        CASE
            WHEN event_user_revision_count < 5 THEN '1-4'
            WHEN event_user_revision_count >= 5 AND event_user_revision_count < 100 THEN '5-99'
            WHEN event_user_revision_count >= 100 AND event_user_revision_count < 1000 THEN '100-999'
            WHEN event_user_revision_count >= 1000 AND event_user_revision_count < 10000 THEN '1000-9999'
            WHEN event_user_revision_count >= 10000 THEN '10000+'
            ELSE 'Undefined'
        END AS user_edit_count_bucket,
        CASE
            WHEN array_contains(revision_tags, 'ios app edit') THEN 'iOS'
            WHEN array_contains(revision_tags, 'android app edit') THEN 'Android'
            WHEN array_contains(revision_tags, 'mobile app edit') AND event_timestamp <'2018-07-01' THEN 'Mobile App'
            WHEN array_contains(revision_tags, 'mobile web edit') THEN 'Mobile web'
            ELSE 'Other'
        END AS platform,
        CASE
            WHEN array_contains(revision_tags, 'visualeditor') THEN 'VisualEditor'
            WHEN array_contains(revision_tags, 'visualeditor-wikitext') THEN '2017 wikitext editor'
            WHEN array_contains(revision_tags, 'visualeditor-switched') THEN 'Switched from VisualEditor to wikitext editor'
            WHEN array_contains(revision_tags, 'wikieditor') THEN '2010 wikitext editor'
            ELSE 'Other'
        END AS interface,
        revision_tags,
        revision_text_bytes_diff AS text_bytes_diff
    FROM edit_history
)

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(snapshot='${snapshot}')
    SELECT
        ts,
        SUBSTR(w.domain_name, 0, LENGTH(w.domain_name) - 4) AS project,
        w.database_group AS project_family,
        w.language_name AS language,
        user_is_anonymous,
        user_is_bot,
        user_is_administrator,
        user_groups,
        user_tenure_bucket,
        m.namespace_is_content,
        namespace_is_talk,
        namespace_name,
        namespace_id,
        creates_new_page,
        is_deleted,
        is_reverted,
        is_redirect_currently,
        user_edit_count_bucket,
        platform,
        interface,
        revision_tags,
        COUNT(*) AS edit_count,
        SUM(text_bytes_diff) AS text_bytes_diff
    FROM formatted_edit_history AS m
    LEFT JOIN ${wiki_map_table} AS w ON
        m.wiki_db = w.database_code
    GROUP BY
        ts,
        SUBSTR(w.domain_name, 0, LENGTH(w.domain_name) - 4),
        w.database_group,
        w.language_name,
        user_is_anonymous,
        user_is_bot,
        user_is_administrator,
        user_groups,
        user_tenure_bucket,
        m.namespace_is_content,
        namespace_is_talk,
        namespace_name,
        namespace_id,
        creates_new_page,
        is_deleted,
        is_reverted,
        is_redirect_currently,
        user_edit_count_bucket,
        platform,
        interface,
        revision_tags
;
