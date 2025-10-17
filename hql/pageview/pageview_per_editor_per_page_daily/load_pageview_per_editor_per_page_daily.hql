-- Joins edit_per_editor_per_page_daily_table with pageview_hourly
-- to calculate daily per editor pageview counts.
-- See create_pageview_per_editor_per_page_daily.hql for more docs.
--
-- https://phabricator.wikimedia.org/T405039
--
-- Load timing:
--   Loading a day of data should take less than 2 minutes.
--
-- Parameters:
--      edit_per_editor_per_page_daily_table
--          Fully qualified table name of edits_per_editor_per_page_daily table.
--          This table is used to lookup all distinct pages each user has edited on or before end_day.
--
--      pageview_hourly_table
--          Fully qualified pageview_hourly table name.
--          This table is used to sum daily pageviews for each page.

--      destination_table
--          Fully qualified table name to write data to.
--
--      start_day
--          Start day from which to begin daily load.
--          In yyyy-MM-dd format (e.g. 2025-01-01)
--
--      end_day
--          End day on which to end daily load.
--          This is exclusive, so the load will run up to but not including this day.
--          In yyyy-MM-dd format (e.g. 2026-01-01)
--
-- Usage:
--     spark3-sql -f pageview_per_editor_per_page_daily_load.hql \
--         -d edit_per_editor_per_page_daily_table='wmf_contributors.edit_per_editor_per_page_daily' \
--         -d pageview_hourly_table='wmf.pageview_hourly' \
--         -d destination_table='wmf_readership.pageview_per_editor_per_page_daily' \
--         -d start_day=2025-10-25 \
--         -d end_day=2025-10-26
--
-- This query performs the following steps using CTEs:
--
-- 1. pageviews_daily
--    Sum daily per page pageviews for the target date range.
-- 2. all_pages_edited_ever
--    Get all distinct pages each user has edited on or before end_day.
-- 3. latest_edit_day_per_page
--    Find the latest day each user edited each page before end_day.
-- 4. latest_user_data
--    Lookup the mutable user fields from the latest edit day record for each user/page combination.
-- 5. Join pageviews_daily with all_pages_edited_ever and latest_user_data to
--    INSERT INTO ${destination_table}


-- Convert from input day string param to DATE
SET start_day_date = to_date('${start_day}', 'yyyy-MM-dd');
SET end_day_date = to_date('${end_day}', 'yyyy-MM-dd');

-- Delete any data that will be regenerated to avoid duplicates
DELETE FROM ${destination_table}
WHERE
    `day` >= ${start_day_date}
    AND `day` < ${end_day_date}
;

WITH

-- Sum daily per page pageviews for the target date range
pageviews_daily AS (
    SELECT
        make_date(year, month, day) AS `day`,
        project,
        page_id,
        SUM(view_count) AS view_count
    FROM ${pageview_hourly_table}
    WHERE
        -- In order to filter for a date range using Hive date partition values,
        -- We convert the partition to a date, and then filter using the
        -- start and end dates.
        -- NOTE: Even though we are filtering on a conversion from Hive partition values,
        --       these are still understood by spark as PartitionFilters for partition pruning.
        --       See: https://wikimedia.slack.com/archives/CSV483812/p1762194061181539
        make_date(year, month, day) >= ${start_day_date}
        AND make_date(year, month, day) < ${end_day_date}
      -- Only count user pageviews, exclude bots/spiders
      AND agent_type = 'user'
      -- Only pages with valid page_id.
      -- Note that this is likely to miss 'views' from mobile apps
      -- See also 'page_id' docs at
      -- https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageview_hourly
      AND page_id IS NOT NULL
    GROUP BY
        make_date(year, month, day),
        project,
        page_id
),


-- Get all distinct pages each user has edited on or before end_day.
-- from the edit_per_editor_per_page_daily table.
all_pages_edited_ever AS (
    SELECT DISTINCT
        user_central_id,
        wiki_id,
        -- We need to include pageview_project to join with pageview_hourly.
        pageview_project,
        page_id
    FROM ${edit_per_editor_per_page_daily_table}
    WHERE day < ${end_day_date}
),

-- Find the latest day each user edited each page before end_day.
-- This will be used to look up the latest values for mutable user fields
-- like user_name and user_is_bot.
latest_edit_day_per_page AS (
    SELECT
        user_central_id,
        wiki_id,
        page_id,
        MAX(day) AS latest_edit_day
    FROM ${edit_per_editor_per_page_daily_table}
    WHERE
        day < ${end_day_date}
    GROUP BY
        user_central_id,
        wiki_id,
        page_id
),

-- Lookup the mutable user fields from the latest edit day record for each user/page combination.
latest_user_data AS (
    SELECT
        e.user_central_id,
        e.wiki_id,
        e.page_id,
        e.user_id,
        e.user_name,
        e.user_is_bot,
        e.user_is_system,
        e.wiki,
        e.pageview_project
    FROM ${edit_per_editor_per_page_daily_table} e
    INNER JOIN latest_edit_day_per_page le
        ON e.user_central_id = le.user_central_id
        AND e.wiki_id = le.wiki_id
        AND e.page_id = le.page_id
        AND e.day = le.latest_edit_day
)

-- Insert daily pageview_per_editor_per_page_daily table
INSERT INTO ${destination_table}
SELECT
    pd.day,
    lud.user_central_id,
    lud.user_id,
    lud.user_name,
    lud.user_is_bot,
    lud.user_is_system,
    lud.wiki_id,
    lud.wiki,
    lud.pageview_project,
    lud.page_id,
    pd.view_count
FROM all_pages_edited_ever ape
INNER JOIN latest_user_data lud
    ON ape.user_central_id = lud.user_central_id
    AND ape.wiki_id = lud.wiki_id
    AND ape.page_id = lud.page_id
LEFT JOIN pageviews_daily pd
    ON ape.pageview_project = pd.project
    AND ape.page_id = pd.page_id
WHERE
  -- Only include pages that received views
  pd.view_count > 0 AND pd.view_count IS NOT NULL
SORT BY
    -- Finally sort by day partition to avoid spark partition / iceberg partition mismatch.
    -- See: https://iceberg.apache.org/docs/latest/spark-writes/#writing-distribution-modes
    day
;
