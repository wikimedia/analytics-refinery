-- Generates unique devices per-project-family daily based on WMF-Last-Access-global cookie
--
-- Parameters:
--     source_table        -- Table containing source data
--     destination_table   -- Table where to write newly computed data
--     year                -- year of the to-be-generated
--     month               -- month of the to-be-generated
--     day                 -- day of the to-be-generated
--
-- Usage:
--     hive -f unique_devices_per_project_family_daily.hql \
--         -d source_table=wmf.pageview_actor \
--         -d destination_table=wmf.unique_devices_per_project_family_daily \
--         -d year=2017 \
--         -d month=4 \
--         -d day=1


-- Set parquet compression codec
SET parquet.compression              = SNAPPY;


WITH last_access_dates AS (
    SELECT
        year,
        month,
        day,
        normalized_host.project_class AS project_family,
        geocoded_data['country'] AS country,
        geocoded_data['country_code'] AS country_code,
        unix_timestamp(x_analytics_map['WMF-Last-Access-Global'], 'dd-MMM-yyyy') AS last_access_global,
        x_analytics_map['nocookies'] AS nocookies,
        actor_signature_per_project_family AS actor_signature
    FROM ${source_table}
    WHERE x_analytics_map IS NOT NULL
      AND agent_type = 'user'
      AND (is_pageview OR is_redirect_to_pageview)
      AND year = ${year}
      AND month = ${month}
      AND day = ${day}
),

-- Only keeping clients having 1 event without cookies
-- (fresh sessions are not counted with last_access method)
fresh_sessions_aggregated AS (
    SELECT
        project_family,
        country,
        country_code,
        COUNT(1) AS uniques_offset
    FROM (
        SELECT
            actor_signature,
            project_family,
            country,
            country_code,
            SUM(CASE WHEN (nocookies IS NOT NULL) THEN 1 ELSE 0 END)
        FROM
            last_access_dates
        GROUP BY
            actor_signature,
            project_family,
            country,
            country_code
        -- Only keeping clients having done 1 event without cookies
        HAVING SUM(CASE WHEN (nocookies IS NOT NULL) THEN 1 ELSE 0 END) = 1
        ) fresh_sessions
    GROUP BY
        project_family,
        country,
        country_code
)
INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year = ${year}, month = ${month}, day = ${day})

SELECT
    COALESCE(la.project_family, fresh.project_family) as project_family,
    COALESCE(la.country, fresh.country) as country,
    COALESCE(la.country_code, fresh.country_code) as country_code,
    SUM(CASE
        -- project_family set, last-access-global not set and client accept cookies --> first visit, count
        WHEN (la.project_family IS NOT NULL AND la.last_access_global IS NULL AND la.nocookies is NULL) THEN 1
        -- last-access-global set and its date is before today --> First visit today, count
        WHEN ((la.last_access_global IS NOT NULL)
            AND (la.last_access_global < unix_timestamp(CONCAT('${year}-', LPAD('${month}', 2, '0'), '-', LPAD('${day}', 2, '0')), 'yyyy-MM-dd'))) THEN 1
        -- Other cases, don't count
        ELSE 0
    END) AS uniques_underestimate,
    COALESCE(fresh.uniques_offset, 0) AS uniques_offset,
    SUM(CASE
        -- project_family set, last-access-global not set and client accept cookies --> first visit, count
        WHEN (la.project_family IS NOT NULL AND la.last_access_global IS NULL AND la.nocookies is NULL) THEN 1
        -- last-access-global set and its date is before today --> First visit today, count
        WHEN ((la.last_access_global IS NOT NULL)
            AND (la.last_access_global < unix_timestamp(CONCAT('${year}-', LPAD('${month}', 2, '0'), '-', LPAD('${day}', 2, '0')), 'yyyy-MM-dd'))) THEN 1
        -- Other cases, don't count
        ELSE 0
    END) + COALESCE(fresh.uniques_offset, 0) AS uniques_estimate
FROM
    last_access_dates AS la
    -- Outer join to keep every row from both table
    FULL OUTER JOIN fresh_sessions_aggregated AS fresh
        ON (la.project_family = fresh.project_family
          AND la.country_code = fresh.country_code)
GROUP BY
    COALESCE(la.project_family, fresh.project_family),
    COALESCE(la.country, fresh.country),
    COALESCE(la.country_code, fresh.country_code),
    COALESCE(fresh.uniques_offset, 0)
-- TODO
-- Add HAVING clause to restrict on long tail (maybe ?)
--
-- Limit enforced by hive strict mapreduce setting.
-- 1000000000 == NO LIMIT !
LIMIT 1000000000;

