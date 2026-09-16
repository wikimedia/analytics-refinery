-- Loads top k viewed pages per editor for a 30-day window,
-- ranked separately per project (wiki), DIRECTLY into the AQS Cassandra table.
--
-- The source table (pageview_per_editor_per_page_daily) contains daily per editor per page
-- pageview counts. This script aggregates across the 30 days ending at end_day (exclusive)
-- to get per user per wiki per page totals, then ranks pages by view_count within each
-- (user, wiki) to keep the top k pages per editor per project, and writes them straight
-- into Cassandra.
--
-- It writes to the SAME Cassandra table as the monthly variant, distinguished by
-- granularity = 'sliding_30' and dt = window_start (= end_day - 30). The window_end
-- (= dt + 30) is not stored; the serving layer derives it.
--
-- There is no Hive/Iceberg staging: the sliding windows are transient (only the latest is
-- served, and old windows expire via the Cassandra TTL set by the DAG), so there is no
-- lasting data-lake product to keep.
--
-- https://phabricator.wikimedia.org/T433983
--
--
-- Parameters:
--     source_table
--          Fully qualified pageview_per_editor_per_page_daily table.
--          Schema: day, user_central_id, user_id, user_name, user_is_bot, user_is_system,
--                  wiki_id, wiki, pageview_project, page_id, view_count
--     destination_table
--         Fully qualified Cassandra table to write data to (aqs catalog).
--         Schema: user_central_id, granularity, wiki_id, page_id, rank, top_k, view_count, dt
--     end_day
--         Exclusive upper bound of the window in yyyy-MM-dd format (e.g. 2026-09-01).
--         The window covers [end_day - 30, end_day).
--     top_k
--         Number of top page records per user per wiki for the window.
--     coalesce_partitions
--         Number of parallel Cassandra loaders (= number of AQS cassandra hosts, e.g. 12).
--
-- Usage:
--     spark3-sql -f load_cassandra_pageviews_sliding_30_top_pages_per_editor.hql \
--         -d source_table='wmf_readership.pageview_per_editor_per_page_daily' \
--         -d destination_table='aqs.analytics.pageviews_top_pages_per_editor' \
--         -d end_day=2026-09-01 \
--         -d top_k=10 \
--         -d coalesce_partitions=12

SET end_day_date = TO_DATE('${end_day}', 'yyyy-MM-dd');
SET window_start_date = DATE_SUB(TO_DATE('${end_day}', 'yyyy-MM-dd'), 30);
-- dt points to the start of the window (window_start).
SET current_dt = CAST(${window_start_date} AS TIMESTAMP);

WITH

-- Sum per editor per page pageviews across the 30-day window.
windowed_pageviews_per_editor_per_page AS (
    SELECT
        user_central_id,
        wiki_id,
        page_id,
        SUM(view_count) AS view_count
    FROM ${source_table}
    WHERE
        day >= ${window_start_date}
        AND day < ${end_day_date}
    GROUP BY
        user_central_id,
        wiki_id,
        page_id
),


-- Order each editor's pages by view_count, separately per wiki,
-- rn breaks rank ties so exactly top_k rows survive the filter below.
ranked_pages_per_editor_per_wiki AS (
    SELECT
        user_central_id,
        wiki_id,
        page_id,
        RANK() OVER (
            PARTITION BY user_central_id, wiki_id
            ORDER BY view_count DESC
        ) AS rank,
        ROW_NUMBER() OVER (
            PARTITION BY user_central_id, wiki_id
            ORDER BY view_count DESC
        ) AS rn,
        view_count
    FROM windowed_pageviews_per_editor_per_page
)

-- Insert the top k pages per editor per project straight into Cassandra.
INSERT INTO ${destination_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
    user_central_id,
    'sliding_30' AS granularity,
    wiki_id,
    page_id,
    rank,
    CAST(${top_k} AS INT) AS top_k,
    view_count,
    ${current_dt} AS dt
FROM ranked_pages_per_editor_per_wiki
WHERE rn <= ${top_k}
;
