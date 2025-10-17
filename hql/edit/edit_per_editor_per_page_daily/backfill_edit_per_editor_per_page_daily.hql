-- Backfills edit_per_editor_per_page_daily from mediawiki_history.
--
-- This query should operate functionally similar as the load query,
-- except the source table is mediawiki_history instead of mediawiki_page_change_v1.
-- In a magical future when we have an incremental mediawiki history table, or an
-- eventually consistent and backfilled mediawiki_page_change, this backfill
-- could be accomplished using the same table source as the daily load.
--
-- This query processes historical data from mediawiki_history to populate
-- edit_per_editor_per_page_daily. The mediawiki_history table is partitioned
-- by snapshot (YYYY-MM format), and snapshots are NOT incremental.
-- This backfill should only need to be run once to populate the history
-- of edit_per_editor_per_page_daily.  Ongoing daily updates will be
-- loaded from an incremental page/edit event data source like
-- mediawiki_page_change_v1 or mediawiki_content_history_v1.
--
-- For more information about mediawiki_history:
-- https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Mediawiki_history
--
-- Parameters:
--     source_table
--          Fully qualified mediawiki_history table name
--          (e.g., 'wmf.mediawiki_history')
--
--     mediawiki_history_snapshot
--          Snapshot partition to query (YYYY-MM format, e.g., '2024-04')
--          Note: Snapshots are NOT incremental, so all data up to that
--          snapshot date is present in the partition.
--
--     canonical_data_wikis_table
--          Fully qualified canonical_data.wikis table name
--          (e.g., 'canonical_data.wikis')
--          This is used to lookup the wiki and pageview_project values.

--     destination_table
--          Fully qualified table name to write data to
--
--     start_day
--          Start day from which to begin daily load.
--          In yyyy-MM-dd format (e.g. 2025-01-01)
--
--     end_day
--          End day on which to end daily load.
--          This is exclusive, so the load will run up to but not including this day.
--          In yyyy-MM-dd format (e.g. 2026-01-01)


-- Usage:
--     spark3-sql -f backfill_edit_per_editor_per_page_daily.hql \
--         -d source_table='wmf.mediawiki_history' \
--         -d mediawiki_history_snapshot='2025-09' \
--         -d canonical_data_wikis_table='canonical_data.wikis' \
--         -d destination_table='wmf_contributors.edit_per_editor_per_page_daily' \
--         -d start_day=2025-01-01 \
--         -d end_day=2026-01-01


-- Convert from input day string params to DATE
SET start_day_date = to_date('${start_day}', 'yyyy-MM-dd');
SET end_day_date = to_date('${end_day}', 'yyyy-MM-dd');

-- Delete any data that will be regenerated to avoid duplicates
DELETE FROM
    ${destination_table}
WHERE
    `day` >= ${start_day_date}
    AND `day` < ${end_day_date}
;

WITH

-- Filter and transform data from mediawiki_history to
-- edit_per_editor_per_page_daily format
all_edits AS (
    SELECT
        to_date(event_timestamp) as `day`,
        event_user_central_id as user_central_id,
        event_user_id as user_id,
        -- Use historical user_text (as it was at the time of the edit)
        event_user_text_historical as user_name,
        -- Derive user_is_bot from event_user_is_bot_by_historical array
        -- User is bot if array contains 'group' or 'name'
        array_contains(event_user_is_bot_by_historical, 'group')
            OR array_contains(event_user_is_bot_by_historical, 'name') as user_is_bot,
        -- System users are those created by the system
        event_user_is_created_by_system as user_is_system,
        -- wiki_db directly maps to wiki_id (e.g., 'enwiki', 'dewiki')
        wiki_db as wiki_id,
        -- Map wiki and pageview_project via canonical_data.wikis
        cdw.domain_name as wiki,
        cdw.pageview_code as pageview_project,
        -- Use historical page_namespace (as it was at the time of the edit)
        page_namespace_historical as page_namespace_id,
        page_id,
        -- Use historical page_title (as it was at the time of the edit)
        page_title_historical as page_title,
        -- revision_id and event_timestamp will be used to lookup the latest values for mutable fields
        revision_id,
        event_timestamp
    FROM ${source_table}
    LEFT JOIN ${canonical_data_wikis_table} cdw
        ON wiki_db = cdw.database_code
    WHERE
        snapshot = '${mediawiki_history_snapshot}'
        -- Filter for our time range.
        AND to_date(event_timestamp) >= ${start_day_date}
        AND to_date(event_timestamp) < ${end_day_date}
        -- Filter for revision creation events (edits)
        AND event_entity = 'revision'
        AND event_type = 'create'
        -- Exclude temp users
        AND NOT event_user_is_temporary
        -- Exclude anonymous users
        AND NOT event_user_is_anonymous
        -- Only include registered permanent users
        AND event_user_id IS NOT NULL
        AND event_user_id > 0
        AND event_user_is_permanent = true
        -- user_central_id is needed to calculate global editor metrics.
        AND event_user_central_id IS NOT NULL

),

-- Get the max (revision_id + event_timestamp) for each user_central_id, page_id combination per day
-- This will be used to lookup latest values of the mutable fields in the event data.
-- MAX(CONCAT(revision_id, event_timestamp)) is used to ensure uniqueness, especially
-- for undeletes where rev_id or event_timestamp alone might not be distinct.
latest_edit_join_key AS (
    SELECT
        `day`,
        user_central_id,
        wiki_id,
        page_id,
        MAX(CONCAT(revision_id, event_timestamp)) as latest_rev_id_event_time
    FROM all_edits
    GROUP BY
        `day`,
        user_central_id,
        wiki_id,
        page_id
),

-- Look up latest values for mutable fields in all_edits by joining with latest_edit_join_key.
-- The latest mutable values will be used to ensure there is a single edit_count
-- record per user_central_id, wiki_id, page_id combination per day,
-- handling cases like page moves and user renames.
latest_edit_values AS (
    SELECT
        ae.`day`,
        ae.user_central_id,
        ae.wiki_id,
        ae.page_id,
        ae.user_id,
        ae.user_name,
        ae.user_is_bot,
        ae.user_is_system,
        ae.wiki,
        ae.pageview_project,
        ae.page_namespace_id,
        ae.page_title
    FROM all_edits ae
    INNER JOIN latest_edit_join_key lejk
        ON ae.`day` = lejk.`day`
        AND ae.user_central_id = lejk.user_central_id
        AND ae.wiki_id = lejk.wiki_id
        AND ae.page_id = lejk.page_id
        -- Ensure we only select the latest record for each user_central_id, page_id combination
        AND CONCAT(ae.revision_id, ae.event_timestamp) = lejk.latest_rev_id_event_time
),

-- Get total edit counts per user_central_id, page_id (not split by mutable fields)
edit_counts AS (
    SELECT
        `day`,
        user_central_id,
        wiki_id,
        page_id,
        count(*) as edit_count
    FROM all_edits
    GROUP BY
        `day`,
        user_central_id,
        wiki_id,
        page_id
)

-- Save the result:
-- Combine latest daily values for mutable fields,
-- with total daily edit counts per user_central_id, wiki_id, page_id.
INSERT INTO ${destination_table}
SELECT
    lev.`day`,
    lev.user_central_id,
    lev.user_id,
    lev.user_name,
    lev.user_is_bot,
    lev.user_is_system,
    lev.wiki_id,
    lev.wiki,
    lev.pageview_project,
    lev.page_namespace_id,
    lev.page_id,
    lev.page_title,
    ec.edit_count
FROM latest_edit_values lev
INNER JOIN edit_counts ec
    ON lev.`day` = ec.`day`
    AND lev.user_central_id = ec.user_central_id
    AND lev.wiki_id = ec.wiki_id
    AND lev.page_id = ec.page_id
;
