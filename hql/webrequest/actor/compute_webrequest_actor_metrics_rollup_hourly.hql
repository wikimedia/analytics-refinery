-- Compute the 24h rollup of metrics per webrequest actor every hour

-- Code calculates per actor the values for metrics for the 24 hours prior. While
-- we run our classification per hour, the metrics used to provide a label use
-- data for the past 24 hours, interval start excluded.

-- Example: if we are computing the rollup at year=2019, month=1, day=3 and hour=3
-- the values for the start of the interval would be:
-- interval_start_year= 2019, interval_start_month=1, interval_start_day=2, interval_start_hour=3


-- See: https://docs.google.com/document/d/1q14GH7LklhMvDh0jwGaFD4eXvtQ5tLDmw3UeFTmb3KM/edit
-- Parameters:
--     source_table            -- Fully qualified table name to compute the
--                                aggregation from.
--     destination_table       -- Fully qualified table name to fill in
--                                aggregated values.
--     coalesce_partitions     -- The number of files to write per hour

--     interval_start_year     -- the value for year from which we start the rollup
--     interval_start_month    -- the value for the month from which we start the rollup
--     interval_start_day      -- the value for the day from which we start the rollup
--     interval_start_hour     -- the value for the hour from which we start the rollup

--     Values below also coincide with partition values
--     interval_end_year       -- the value for year that represents the end of rollup window
--     interval_end_month      -- the value for month that represents the end of rollup window
--     interval_end_day        -- the value for day that represents the end of rollup window
--     interval_end_hour       -- the value for hour that represents end of rollup window

-- Usage:
--     spark3-sql -f compute_webrequest_actor_metrics_rollup_hourly.hql    \
--         -d source_table=wmf.webrequest_actor_metrics_hourly             \
--         -d destination_table=wmf.webrequest_actor_metrics_rollup_hourly \
--         -d version=0.1              \
--         -d interval_end_year=2023   \
--         -d interval_end_month=2     \
--         -d interval_end_day=1       \
--         -d interval_end_hour=1      \
--         -d interval_start_year=2023 \
--         -d interval_start_month=1   \
--         -d interval_start_day=31    \
--         -d interval_start_hour=2    \
--         -d coalesce_partitions=16

WITH actor_aggregated AS (
    SELECT
        ${version} as version,
        actor_signature,
        sum(pageview_count) as pageview_count,
        cast((sum(pageview_count)/(unix_timestamp(max(last_interaction_dt)) - unix_timestamp( min(first_interaction_dt))) * 60) as int) as pageview_rate_per_min,
        sum(coalesce(nocookies, 0L)) as nocookies,
        max(user_agent_length) as user_agent_length,
        avg(distinct_pages_visited_count) as avg_distinct_pages_visited_count,
        -- Note: Any of the above feature can be used to compute rolled_up_hours, we had to pick one
        SUM(CASE WHEN COALESCE(distinct_pages_visited_count, 0) > 0 THEN 1 ELSE 0 END) AS rolled_up_hours

    FROM ${source_table}

    WHERE
        (year=${interval_start_year} AND month=${interval_start_month} AND day=${interval_start_day} AND hour>${interval_start_hour})
        OR (year=${interval_end_year} AND month=${interval_end_month} AND day=${interval_end_day} AND hour<=${interval_end_hour})

    GROUP BY
        actor_signature

)

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION (year=${interval_end_year},month=${interval_end_month},day=${interval_end_day},hour=${interval_end_hour})

    SELECT /*+ COALESCE(${coalesce_partitions}) */
        version,
        actor_signature,
        pageview_count,
        pageview_rate_per_min,
        nocookies,
        user_agent_length,
        avg_distinct_pages_visited_count,
        rolled_up_hours

    FROM actor_aggregated;
