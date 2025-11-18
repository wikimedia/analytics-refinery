-- Loads top k viewed pages per editor for a month from source table to destination table.
--
-- The source table (pageview_per_editor_per_page_daily) contains daily per editor per page
-- pageview counts. This script aggregates across all days in a month to get monthly totals
-- per user per page, then ranks pages by view_count to get the top k pages per user.
--
-- https://phabricator.wikimedia.org/T410289
--
-- Load timing:
--   Loading a month of data should take less than 10 minutes.
--
-- Parameters:
--     pageview_per_editor_per_page_daily_table
--          Fully qualified pageview_per_editor_per_page_daily table.
--          This table contains daily per editor per page pageview counts.
--          Schema: day, user_central_id, user_id, user_name, user_is_bot, user_is_system,
--                  wiki_id, wiki, pageview_project, page_id, view_count
--     destination_table
--         Fully qualified table name to write data to.
--         Schema: user_central_id, granularity, wiki_id,
--                 page_id, rank, top_k, view_count, dt
--     year_month
--         Year-Month to load data for in YYYY-MM format (e.g. 2025-10)
--     top_k
--         Number of top page records per user for the month
--
-- Usage:
--     spark3-sql -f load_pageviews_top_pages_per_editor.hql \
--         -d pageview_per_editor_per_page_daily_table='wmf_readership.pageview_per_editor_per_page_daily' \
--         -d destination_table='wmf_readership.pageviews_top_pages_per_editor' \
--         -d year_month=2025-10 \
--         -d top_k=10

INSERT INTO ${destination_table}
SELECT
    user_central_id,
    'monthly' AS granularity,
    wiki_id,
    page_id,
    rank,
    CAST(${top_k} AS INT) AS top_k,
    view_count,
    CAST(TO_DATE('${year_month}-01', 'yyyy-MM-dd') AS TIMESTAMP) AS dt
FROM (
    WITH monthly_pageviews_per_editor_per_page AS (
        SELECT
            user_central_id,
            wiki_id,
            page_id,
            SUM(view_count) AS view_count
        FROM ${pageview_per_editor_per_page_daily_table}
        WHERE
            day >= TO_DATE('${year_month}-01', 'yyyy-MM-dd')
            AND day < ADD_MONTHS(TO_DATE('${year_month}-01', 'yyyy-MM-dd'), 1)
        GROUP BY
            user_central_id,
            wiki_id,
            page_id
    )
    SELECT
        user_central_id,
        wiki_id,
        page_id,
        RANK() OVER (
            PARTITION BY user_central_id
            ORDER BY view_count DESC
        ) AS rank,
        ROW_NUMBER() OVER (
            PARTITION BY user_central_id
            ORDER BY view_count DESC
        ) AS rn,
        view_count
    FROM monthly_pageviews_per_editor_per_page
)
WHERE rn <= ${top_k}
ORDER BY
    user_central_id,
    rank
