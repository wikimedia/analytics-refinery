-- A label for every webrequest actor is calculated hourly using 24h rolled up
-- metrics values
--
-- Label can be "user" or "automated"

-- See: https://docs.google.com/document/d/1q14GH7LklhMvDh0jwGaFD4eXvtQ5tLDmw3UeFTmb3KM/edit
-- Parameters:
--     source_table          -- fully qualified table name that holds the
--                              features used
--     destination_table     -- table that holds labels per actor
--     coalesce_partitions   -- The number of files to write per hour
--     year                  -- year of partition to compute label for.
--     month                 -- month of partition to compute label for
--     day                   -- day of partition to compute label for
--     hour                  -- hour of partition to compute label for
--
-- Usage:
--     spark3-sql -f compute_webrequest_actor_label_hourly.hql        \
--         -d source_table=wmf.webrequest_actor_metrics_rollup_hourly \
--         -d destination_table=wmf.webrequest_actor_label_hourly     \
--         -d version=0.1             \
--         -d year=2023                \
--         -d month=2                  \
--         -d day=1                    \
--         -d hour=1                   \
--         -d coalesce_partitions=16

WITH actor_aggregated AS (
    SELECT
        ${version} as version,
        actor_signature_per_project_family,
        -- The following 2 fields were introduced across the pipeline in 2024-11, see: T375527.
        -- The COALESCE statements make the query backwards compatible.
        COALESCE(is_pageview, TRUE) AS is_pageview,
        COALESCE(is_redirect_to_pageview, FALSE) AS is_redirect_to_pageview,
        CASE
            WHEN pageview_count < 10 then 'user - less than 10 req'
            -- For mobile apps data pageviews per device per day percentiles are: p50: 2, p90: 9 and p99: 30 on September 2019
            WHEN pageview_count > 800 then 'automated - more than 800 req'

            -- threshold for bots at 1 pageview every 2 secs
            WHEN pageview_rate_per_min >= 30 then 'automated - high request ratio'

            -- more than 10 requests without cookies and more than 100 pageviews and
            -- small distinct-pages variability
            WHEN (nocookies > 10
                  AND pageview_count > 100
                  -- Multiply hourly-average by number of rolled-up hours to normalize by pageview_count
                  -- The 0.2 magic number here means that actors are flagged as automated if they have visited
                  -- less than 20 different pages over 100 pageviews
                  AND (avg_distinct_pages_visited_count * rolled_up_hours / CAST(pageview_count AS DOUBLE)) < 0.2)
              THEN 'automated - too many requests w/o cookies and small page_title variability'

            WHEN user_agent_length > 400 or user_agent_length < 25 then 'automated - suspicious UA'
            -- low pageview ratios as low as 1 per min
            WHEN pageview_rate_per_min = 0 then 'user - low request ratio'

            ELSE 'unclassified - unclassified '

        END AS label_text

    FROM ${source_table}

    WHERE
        year=${year} and month=${month} and day=${day} and hour=${hour}

)

INSERT OVERWRITE TABLE ${destination_table}
PARTITION (year=${year}, month=${month}, day=${day}, hour = ${hour})

SELECT /*+ COALESCE(${coalesce_partitions}) */
    version,
    NULL AS actor_signature,
    actor_signature_per_project_family,
    is_pageview,
    is_redirect_to_pageview,

    trim(split(label_text,'-')[0])  as label,
    trim(split(label_text, '-')[1]) as label_reason

FROM actor_aggregated;
