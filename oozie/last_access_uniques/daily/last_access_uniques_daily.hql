-- Generates daily uniques based on WMF-Last-Access cookie
--
-- Parameters:
--     source_table        -- Table containing source data
--     destination_table   -- Table where to right newly computed data
--     year                -- year of the to-be-generated
--     month               -- month of the to-be-generated
--     day                 -- day of the to-be-generated
--
-- Usage:
--     hive -f last_access_uniques_daily.hql \
--         -d source_table=wmf.webrequest \
--         -d destination_table=wmf.last_access_uniques_daily \
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
        lower(uri_host) as uri_host,
        geocoded_data['country'] AS country,
        geocoded_data['country_code'] AS country_code,
        unix_timestamp(x_analytics_map['WMF-Last-Access'], 'dd-MMM-yyyy') AS last_access,
        x_analytics_map['nocookies'] AS nocookies,
        ip,
        user_agent,
        accept_language
    FROM ${source_table}
    WHERE x_analytics_map IS NOT NULL
      AND agent_type = 'user'
      AND is_pageview = TRUE
      AND webrequest_source = 'text'
      AND year = ${year}
      AND month = ${month}
      AND day = ${day}
),

-- Only keeping clients having 1 event without cookies (fresh sessions)
fresh_sessions_aggregated AS (
    SELECT
        uri_host,
        country_code,
        COUNT(1) AS uniques_offset
    FROM (
        SELECT
            hash(ip, user_agent, accept_language, uri_host) AS id,
            uri_host,
            country_code,
            COUNT(1) AS cardinal
        FROM
            last_access_dates
        WHERE
            -- Only keeping clients NOT having cookies (yet)
            nocookies is NOT NULL
        GROUP BY
            hash(ip, user_agent, accept_language, uri_host),
            uri_host,
            country_code
        -- Only keeping clients having done 1 event with no cookies
        HAVING COUNT(1) = 1
        ) fresh_sessions
    GROUP BY
        uri_host,
        country_code
)

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year = ${year}, month = ${month}, day = ${day})
SELECT
    la.uri_host,
    la.country,
    la.country_code,
    SUM(CASE
        -- Last access not set and client accept cookies --> first visit, count
        WHEN (la.last_access IS NULL AND la.nocookies is NULL) THEN 1
        -- Last access set and date before today --> First visit today, count
        WHEN ((la.last_access IS NOT NULL)
            AND (la.last_access < unix_timestamp(CONCAT('${year}-', LPAD('${month}', 2, '0'), '-', LPAD('${day}', 2, '0')), 'yyyy-MM-dd'))) THEN 1
        -- Other cases, don't
        ELSE 0
    END) AS uniques_underestimate,
    fresh.uniques_offset AS uniques_offset,
    SUM(CASE
        -- Last access not set and client accept cookies --> first visit, count
        WHEN (la.last_access IS NULL AND la.nocookies is NULL) THEN 1
        -- Last access set and date before today --> First visit today, count
        WHEN ((la.last_access IS NOT NULL)
            AND (la.last_access < unix_timestamp(CONCAT('${year}-', LPAD('${month}', 2, '0'), '-', LPAD('${day}', 2, '0')), 'yyyy-MM-dd'))) THEN 1
        -- Other cases, don't
        ELSE 0
    END) + fresh.uniques_offset AS uniques_estimate
FROM
    last_access_dates AS la
    INNER JOIN fresh_sessions_aggregated AS fresh
        ON (fresh.uri_host = la.uri_host
            AND fresh.country_code = la.country_code)
GROUP BY
    la.uri_host,
    la.country,
    la.country_code,
    fresh.uniques_offset
-- TODO
-- Add HAVING clause to restrict on long tail (maybe ?)
ORDER BY
    uniques_estimate DESC
-- Limit enforced by hive strict mapreduce setting.
-- 1000000000 == NO LIMIT !
LIMIT 1000000000;
