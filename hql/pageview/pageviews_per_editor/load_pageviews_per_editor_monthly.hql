-- Loads aggregated pageview per editor monthly from source table to destination table.
--
-- The source table (pageview_per_editor_per_page_daily) contains daily per editor per page
-- pageview counts. This script aggregates across all pages to get total pageviews per user per day.
--
-- https://phabricator.wikimedia.org/T410289
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
--          Schema: user_central_id, granularity, view_count, dt
--
--      year_month
--          Month to load data for.
--          In yyyy-MM format (e.g. 2025-10)
--
-- Usage:
--     spark3-sql -f load_pageviews_per_editor_monthly.hql \
--         -d pageview_per_editor_per_page_daily_table='wmf_readership.pageview_per_editor_per_page_daily' \
--         -d destination_table='wmf_readership.pageviews_per_editor' \
--         -d year_month=2025-10


SET current_dt = CAST(
    TO_DATE('${year_month}-01', 'yyyy-MM-dd') AS TIMESTAMP
);

-- Delete any existing data so that backfills are idempotent
DELETE
FROM ${destination_table}
WHERE
    dt = ${current_dt}
    AND granularity = 'monthly'
;

INSERT INTO ${destination_table}
SELECT
    user_central_id,
    'monthly' AS granularity,
    SUM(view_count) AS view_count,
    ${current_dt} AS dt
FROM ${pageview_per_editor_per_page_daily_table}
WHERE
    day >= TO_DATE('${year_month}-01', 'yyyy-MM-dd')
     AND day < ADD_MONTHS(TO_DATE('${year_month}-01', 'yyyy-MM-dd'), 1)
GROUP BY
    user_central_id
ORDER BY
    user_central_id
;
