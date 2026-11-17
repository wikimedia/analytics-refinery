-- Loads aggregated pageview per editor per wiki daily from source table to destination table.
--
-- The source table (pageview_per_editor_per_page_daily) contains daily per editor per page
-- pageview counts. This script aggregates across all pages of each wiki to get total pageviews per user per wiki.
--
-- https://phabricator.wikimedia.org/T445117
--
-- Load timing:
--   Loading a day of data should take less than 2 minutes.
--
-- Parameters:
--      pageview_per_editor_per_page_daily_table
--          Fully qualified pageview_per_editor_per_page_daily table.
--          This table contains daily per editor per page pageview counts.
--          Schema: day, user_central_id, user_id, user_name, user_is_bot, user_is_system,
--                  wiki_id, wiki, pageview_project, page_id, view_count
--
--      destination_table
--          Fully qualified table name to write data to.
--          Schema: user_central_id, wiki_id, granularity, view_count, dt
--
--      day
--          Day to load data for.
--          In yyyy-MM-dd format (e.g. 2025-10-25)
--
-- Usage:
--     spark3-sql -f load_pageviews_per_editor_per_wiki_daily.hql \
--         -d pageview_per_editor_per_page_daily_table='wmf_readership.pageview_per_editor_per_page_daily' \
--         -d destination_table='wmf_readership.pageviews_per_editor_per_wiki' \
--         -d day=2025-10-25


SET current_dt = CAST(
    TO_DATE('${day}', 'yyyy-MM-dd') AS TIMESTAMP
);

-- Delete any existing data so that backfills are idempotent
DELETE
FROM ${destination_table}
WHERE
    dt = ${current_dt}
    AND granularity = 'daily'
;

INSERT INTO ${destination_table}
SELECT
    user_central_id,
    wiki_id,
    'daily' AS granularity,
    SUM(view_count) AS view_count,
    ${current_dt} AS dt
FROM ${pageview_per_editor_per_page_daily_table}
WHERE
    day = TO_DATE('${day}', 'yyyy-MM-dd')
GROUP BY
    user_central_id,
    wiki_id
ORDER BY
    user_central_id,
    wiki_id
;
