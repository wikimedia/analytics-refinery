-- Generates daily per-domain unique devices based on WMF-Last-Access cookie
--
-- Parameters:
--     source_table        -- Table containing source data
--     destination_table   -- Table where to write newly computed data
--     year                -- year of the to-be-generated
--     month               -- month of the to-be-generated
--     day                 -- day of the to-be-generated
--
-- Usage:
--     hive -f unique_devices_per_domain_daily.hql \
--         -d source_table=wmf.pageview_actor \
--         -d destination_table=wmf.unique_devices_per_domain_daily \
--         -d year=2016 \
--         -d month=1 \
--         -d day=1



-- Set parquet compression codec
SET parquet.compression              = SNAPPY;

WITH last_access_dates AS (
    SELECT
        year,
        month,
        day,
        lower(uri_host) AS domain,
        geocoded_data['country'] AS country,
        geocoded_data['country_code'] AS country_code,
        unix_timestamp(x_analytics_map['WMF-Last-Access'], 'dd-MMM-yyyy') AS last_access,
        x_analytics_map['nocookies'] AS nocookies,
        actor_signature
    FROM ${source_table}
    WHERE x_analytics_map IS NOT NULL
      AND agent_type = 'user'
      AND is_pageview = TRUE
      AND year = ${year}
      AND month = ${month}
      AND day = ${day}
),

-- Only keeping clients having 1 event without cookies
-- (fresh sessions are not counted when calculating the underestimate via last_access_cookie setting)
fresh_sessions_aggregated AS (
    SELECT
        domain,
        country,
        country_code,
        COUNT(1) AS uniques_offset
    FROM (
        SELECT
            actor_signature,
            domain,
            country,
            country_code,
            SUM(CASE WHEN (nocookies IS NOT NULL) THEN 1 ELSE 0 END)
        FROM
            last_access_dates
        GROUP BY
            actor_signature,
            domain,
            country,
            country_code
        -- Only keeping clients having done 1 event without cookies
        HAVING SUM(CASE WHEN (nocookies IS NOT NULL) THEN 1 ELSE 0 END) = 1
        ) fresh_sessions
    GROUP BY
        domain,
        country,
        country_code
)

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year = ${year}, month = ${month}, day = ${day})
SELECT
    COALESCE(la.domain, fresh.domain) AS domain,
    COALESCE(la.country, fresh.country) AS country,
    COALESCE(la.country_code, fresh.country_code) AS country_code,
    SUM(CASE
        -- domain defined (not null from outer join)
        -- Last access not set and client accept cookies --> first visit, count
        WHEN (la.domain IS NOT NULL AND la.last_access IS NULL AND la.nocookies is NULL) THEN 1
        -- Last access set and date before today --> First visit today, count
        WHEN ((la.last_access IS NOT NULL)
            AND (la.last_access < unix_timestamp(CONCAT('${year}-', LPAD('${month}', 2, '0'), '-', LPAD('${day}', 2, '0')), 'yyyy-MM-dd'))) THEN 1
        -- Other cases, don't
        ELSE 0
    END) AS uniques_underestimate,
    COALESCE(fresh.uniques_offset, 0) AS uniques_offset,
    SUM(CASE
        -- domain defined (not null from outer join)
        -- Last access not set and client accept cookies --> first visit, count
        WHEN (la.domain IS NOT NULL AND la.last_access IS NULL AND la.nocookies is NULL) THEN 1
        -- Last access set and date before today --> First visit today, count
        WHEN ((la.last_access IS NOT NULL)
            AND (la.last_access < unix_timestamp(CONCAT('${year}-', LPAD('${month}', 2, '0'), '-', LPAD('${day}', 2, '0')), 'yyyy-MM-dd'))) THEN 1
        -- Other cases, don't
        ELSE 0
    END) + COALESCE(fresh.uniques_offset, 0) AS uniques_estimate
FROM
    last_access_dates AS la
    FULL OUTER JOIN fresh_sessions_aggregated AS fresh
        ON (fresh.domain = la.domain
            AND fresh.country_code = la.country_code)
GROUP BY
    COALESCE(la.domain, fresh.domain),
    COALESCE(la.country, fresh.country),
    COALESCE(la.country_code, fresh.country_code),
    COALESCE(fresh.uniques_offset, 0)
-- TODO
-- Add HAVING clause to restrict on long tail (maybe ?)
--
-- Limit enforced by hive strict mapreduce setting.
-- 1000000000 == NO LIMIT !
LIMIT 1000000000;
