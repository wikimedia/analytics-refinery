-- Counts daily edits to pages edited per user from mediawiki_page_change_v1.
-- Data will be manually backfilled from mediawiki_history.
--
-- NOTE: We would like to use a an incremental eventually consistent dataset like
--       mediawiki_content_history_v1, but this table does not include comprehensive
--       data about edits and editors, like is_bot, etc.
--       See: https://phabricator.wikimedia.org/T405039#11290461
--
-- Load timing:
--   Loading a day of data from mediawiki_page_change_v1
--   should take less than 1 minute.
--
-- Parameters:
--     source_table
--          Fully qualified mediawiki_page_change_v1 table name
--          to extract data from.
--
--     canonical_data_wikis_table
--          Fully qualified canonical_data.wikis table name
--          (e.g., 'canonical_data.wikis')
--          This is used to lookup the wiki and pageview_project values.
--
--     destination_table
--          Fully qualified table name to write data to.
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
--     spark3-sql -f edit_per_editor_per_page_daily_load.hql \
--         -d source_table='event.mediawiki_page_change_v1' \
--         -d canonical_data_wikis_table='canonical_data.wikis' \
--         -d destination_table='wmf_contributors.edit_per_editor_per_page_daily' \
--         -d start_day=2025-10-25 \
--         -d end_day=2025-10-26
--

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

-- Filter and transform data form the source_table (mediawiki_page_change_v1).
-- to the destination_table (edit_per_editor_per_page_daily) format.
all_edits AS (
    SELECT
    -- NOTE: meta.dt is the ingestion time and should match (mostly) with the event (edit) time.
    --       meta.dt is used to partition the source table data.
    --       Since we are filtering on partition date values below,
    --       we use the same partition values to select the `day` value here.
    --       We use meta.dt because is used for Hive partitioning and will
    --       allow us to insert immutable data by day in cases where event time does not
    --       match the ingestion time.
    --       In the case of day time boundary issues where
    --       the event time is shortly before midnight and the ingestion time is shortly after midnight,
    --       some edits may be associated with the next day.
    --       See also: https://phabricator.wikimedia.org/T409105
        make_date(year, month, day) as `day`,
        revision.editor.user_central_id as user_central_id,
        revision.editor.user_id as user_id,
        revision.editor.user_text as user_name,
        revision.editor.is_bot as user_is_bot,
        revision.editor.is_system as user_is_system,
        wiki_id,
        -- Map from wiki_id to wiki and pageview_project via canonical_data.wikis table.
        -- When a new wiki is created, there is a chance that canonical_data.wikis
        -- has not been updated by the time this daily load is run.
        -- Fall back to deriving wiki and pageview_projectfrom meta.domain with regexes
        -- (BTW, This is also how unique_devices wiki and project values are derived from hostnames).
        -- See also: https://wikitech.wikimedia.org/wiki/Event_Platform/Flaws#meta.domain_field
        COALESCE(
            cdw.domain_name,
            CONCAT(
                regexp_extract(meta.domain, '^((?!(www\\.|m\\.))([a-z0-9-_]+\\.))(m\\.)?\\w+\\.org\\.?$$'),
                regexp_extract(meta.domain, '([a-z0-9-_]+)\\.org\\.?$$'),
                '.org'
            )
        ) as wiki,
        COALESCE(
            cdw.pageview_code,
            CONCAT(
                regexp_extract(meta.domain, '^((?!(www\\.|m\\.))([a-z0-9-_]+\\.))(m\\.)?\\w+\\.org\\.?$$'),
                regexp_extract(meta.domain, '([a-z0-9-_]+)\\.org\\.?$$')
            )
        ) as pageview_project,
        page.namespace_id as page_namespace_id,
        page.page_id as page_id,
        page.page_title as page_title,
        -- rev_id and dt will be used to lookup the latest values for mutable fields.
        revision.rev_id as revision_id,
        dt as event_timestamp
    FROM ${source_table}
    LEFT JOIN ${canonical_data_wikis_table} cdw
        ON wiki_id = cdw.database_code
    WHERE
        -- In order to filter for a date range using Hive date partition values,
        -- We convert the partition to a date, and then filter using the
        -- start and end dates.
        -- NOTE: Even though we are filtering on a conversion from Hive partition values,
        --       these are still understood by spark as PartitionFilters for partition pruning.
        --       See: https://wikimedia.slack.com/archives/CSV483812/p1762194061181539
        make_date(year, month, day) >= ${start_day_date}
        AND make_date(year, month, day) < ${end_day_date}
        -- not visibility_change
        AND page_change_kind IN ('create', 'edit', 'undelete', 'move')
        -- no temp account users
        AND NOT revision.editor.is_temp
        -- no anonymous AKA logged out AKA 'IP' editors
        AND revision.editor.user_id IS NOT NULL
        AND revision.editor.user_id > 0
        -- user_central_id is needed to calculate global editor metrics.
        AND revision.editor.user_central_id IS NOT NULL
),

-- Get the max (revision_id + event_timestamp) for each user_central_id, page_id combination
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

-- Look up latest value by latest revision_id for mutable fields in all_edits by joining with latest_edit_join_key.
-- The latest mutable values will be used to ensure there is a single edit_count
-- record per user_central_id, wiki_id, page_id combination,
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
        -- Ensure we only select the latest record for each user_central_id, page_id combination.
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
