-- Create a table storing the reduced version of mediawiki_revision_history
-- to be loaded in Druid
--
-- Usage:
--     spark3-sql --master yarn \
--         --executor-memory 16G \
--         --executor-cores 4 \
--         --driver-memory 16G \
--         --driver-cores 2 \
--         --conf spark.executor.memoryOverhead=4G \
--         --conf spark.dynamicAllocation.maxExecutors=128 \
--         -f compute_mediawiki_revision_history_reduced.hql \
--         -d mediawiki_revision_history_table=wmf_content.mediawiki_revision_history_v1 \
--         -d mw_project_namespace_map_table=wmf_raw.mediawiki_project_namespace_map \
--         -d destination_table=tmp.mediawiki_revision_history_reduced \
--         -d mw_project_namespace_map_snapshot=2025-11
--         -d coalesce_partitions=32
--

DROP TABLE IF EXISTS ${destination_table};

CREATE TABLE ${destination_table}

WITH

    namespace_map AS (
        SELECT DISTINCT
            dbname AS wiki_db,
            namespace,
            namespace_is_content
        FROM ${mw_project_namespace_map_table}
        WHERE TRUE
            AND snapshot = '${mw_project_namespace_map_snapshot}'
    )

SELECT /*+ COALESCE(${coalesce_partitions}) */
    'revision' AS event_entity,
    'create' AS event_type,
    DATE(revision_dt) AS event_timestamp,
    user_central_id,
    CASE
        WHEN (nm.namespace_is_content = 1) THEN 'content'
        ELSE 'non_content'
    END AS page_type,
    count(1) AS events
FROM ${mediawiki_revision_history_table} as mrw
    -- With the INNER JOIN, we remove rows for wikis not yet present in the
    -- mediawiki_project_namespace_map table. this allows us to keep the table
    -- monthly, but prevents very new wikis to show statistics before the beginning
    -- of the new month.
    INNER JOIN namespace_map nm
        ON mrw.wiki_id = nm.wiki_db AND page_namespace_id = nm.namespace
GROUP BY
    DATE(revision_dt),
    user_central_id,
    CASE
        WHEN (nm.namespace_is_content = 1) THEN 'content'
        ELSE 'non_content'
    END
;
