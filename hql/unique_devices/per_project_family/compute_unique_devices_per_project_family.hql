-- Compute unique-devices per-project-family for a [start, end] period
--   and write results to an Iceberg table
--
-- Parameters:
--      refinery_hive_jar_path              -- HDFS path to the refinery-hive jar
--      webrequest_v2_table                 -- Table containing webrequest V2 data
--      canonical_countries_table           -- Table containing country name by country-code
--      destination_table                   -- Table where to write newly computed data
--      start_year                          -- start counting this year, inclusive
--      start_month                         -- start counting this month, inclusive
--      start_day                           -- start counting on this day, inclusive
--      end_year                            -- finish counting this year, exclusive
--      end_month                           -- finish counting this month, exclusive
--      end_day                             -- finish counting on this day, exclusive
--      coalesce_partitions                 -- Number of partitions to write
--
-- Usage:
--   spark3-sql \
--      --master yarn \
--      --deploy-mode client \
--      --driver-cores 1 \
--      --driver-memory 4G \
--      --executor-cores 2 \
--      --executor-memory 8G \
--      --conf spark.dynamicAllocation.maxExecutors=64 \
--      --conf spark.executor.memoryOverhead=2G \
--      --conf spark.yarn.maxAppAttempts=1 \
--      -f compute_unique_devices_per_project_family.hql \
--      -d refinery_hive_jar_path=hdfs:///wmf/refinery/current/artifacts/refinery-hive-shaded.jar \
--      -d webrequest_v2_table=wmf_traffic.webrequest_v2 \
--      -d canonical_countries_table=canonical_data.countries \
--      -d destination_table=wmf_readership.unique_devices_per_project_family_weekly \
--      -d start_year=2026 \
--      -d start_month=4 \
--      -d start_day=13 \
--      -d end_year=2026 \
--      -d end_month=4 \
--      -d end_day=20 \
--      -d coalesce_partitions=1

ADD JAR ${refinery_hive_jar_path};
CREATE TEMPORARY FUNCTION get_actor_signature AS 'org.wikimedia.analytics.refinery.hive.GetActorSignatureUDF';

-- Delete existing data for the period to prevent duplication of data in case of recomputation
DELETE FROM ${destination_table}
WHERE day = make_date(${start_year}, ${start_month}, ${start_day})
;

WITH last_access_dates AS (
    SELECT
        target.second_level_domain AS project_family,
        geo.country_code AS country_code,
        unix_timestamp(client.last_access_per_project_family) AS last_access_global,
        request.has_cookies AS has_cookies,
        get_actor_signature(
            client.ip,
            client.user_agent,
            request.accept_language,
            target.second_level_domain,
            target.uri_query,
            -- Rebuild x_analytics_map subset for backwards compatibility.
            CASE
                WHEN client.app_install_id IS NULL
                THEN cast(map() AS MAP<STRING, STRING>)
                ELSE map('wmfuuid', client.app_install_id)
            END
        ) AS actor_signature
    FROM ${webrequest_v2_table}
    WHERE
        agent_type = 'human' AND
        (request_type = 'pageview' OR request.is_redirect_to_pageview) AND
        dt >= make_timestamp(${start_year}, ${start_month}, ${start_day}, 0, 0, 0) AND
        dt < make_timestamp(${end_year}, ${end_month}, ${end_day}, 0, 0, 0)
),

-- Only keeping clients having 1 event without cookies
-- (fresh sessions are not counted with last_access method)
fresh_sessions_aggregated AS (
    SELECT
        project_family,
        country_code,
        COUNT(1) AS uniques_offset
    FROM (
        SELECT
            actor_signature,
            project_family,
            country_code,
            sum(CASE WHEN has_cookies THEN 0 ELSE 1 END) as no_cookies_count
        FROM last_access_dates
        GROUP BY
            actor_signature,
            project_family,
            country_code
        -- Only keeping clients having done 1 event without cookies
        HAVING no_cookies_count = 1
        ) AS fresh_sessions
    GROUP BY
        project_family,
        country_code
),

-- Aggregate last access uniques before joining with fresh sessions.
-- Otherwise, Spark has trouble joining the data because it's skewed.
-- Also, calculate uniques_underestimate.
last_access_uniques_aggregated AS (
    SELECT
        project_family,
        country_code,
        sum(
            CASE
                -- project_family set, last-access-global not set and client accept cookies --> first visit, count
                WHEN project_family IS NOT NULL AND last_access_global IS NULL AND has_cookies THEN 1
                -- last-access-global set and its date is before today --> First visit today, count
                WHEN (
                    last_access_global IS NOT NULL AND
                    last_access_global < to_unix_timestamp(make_date(${start_year}, ${start_month}, ${start_day}))
                ) THEN 1
                -- Other cases, don't count
                ELSE 0
            END
        ) AS uniques_underestimate
    FROM last_access_dates
    GROUP BY
        project_family,
        country_code
)

INSERT INTO ${destination_table}

-- Join last_access_uniques_aggregated with fresh_sessions_aggregated
-- to calculate uniques_estimate = uniques_underestimate + uniques_offset.
SELECT /*+ COALESCE(${coalesce_partitions}) */
    COALESCE(lau.project_family, frs.project_family) AS project_family,
    COALESCE(ctr.name, '(missing country name)') AS country,
    lau.country_code AS country_code,
    COALESCE(lau.uniques_underestimate, 0) AS uniques_underestimate,
    COALESCE(frs.uniques_offset, 0) AS uniques_offset,
    COALESCE(lau.uniques_underestimate, 0) + COALESCE(frs.uniques_offset, 0) AS uniques_estimate,
    make_date(${start_year}, ${start_month}, ${start_day}) AS day
FROM last_access_uniques_aggregated AS lau
    LEFT JOIN ${canonical_countries_table} AS ctr ON ctr.iso_code = lau.country_code
    -- Outer join to keep every row from both table
    FULL OUTER JOIN fresh_sessions_aggregated AS frs ON (
        lau.project_family = frs.project_family AND
        lau.country_code = frs.country_code
    )
WHERE COALESCE(lau.uniques_underestimate, 0) + COALESCE(frs.uniques_offset, 0) > 0
ORDER BY
    day,
    project_family,
    country_code
;
