-- Compute unique-devices per-domain monthly into Iceberg table
--
-- Parameters:
--     pageview_actor_table                      -- Table containing pageview_actor data
--     countries_table                           -- Table containing country name by country-code
--     unique_devices_destination_table   -- Table where to write newly computed data
--     year                                      -- year of the to-be-generated
--     month                                     -- month of the to-be-generated
--     coalesce_partitions                       -- Number of partitions to write
--
-- Usage:
--     spark3-sql \
--         --master yarn \
--         --deploy-mode client \
--         --driver-cores 1 \
--         --driver-memory 4G \
--         --executor-cores 2 \
--         --executor-memory 4G \
--         --conf spark.dynamicAllocation.maxExecutors=16 \
--         --conf spark.yarn.maxAppAttempts=1 \
--         -f unique_devices_per_domain_monthly_iceberg.hql \
--         -d pageview_actor_table=wmf.pageview_actor \
--         -d countries_table=canonical_data.countries \
--         -d unique_devices_destination_table=wmf_readership.unique_devices_per_domain_monthly \
--         -d year=2022 \
--         -d month=8 \
--         -d coalesce_partitions=1

-- Delete existing data for the period to prevent duplication of data in case of recomputation
DELETE FROM ${unique_devices_destination_table}
WHERE day = TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), '01'), 'yyyy-MM-dd');


WITH last_access_dates AS (
    SELECT
        year,
        month,
        CONCAT(pageview_info['project'], '.org') AS domain,
        access_method,
        geocoded_data['country_code'] AS country_code,
        -- Sometimes (~1 out of 1B times) WMF-Last-Access is corrupted.
        -- and Spark can not parse it. Check for the length of the string.
        IF(length(x_analytics_map['WMF-Last-Access']) = 11,
           unix_timestamp(x_analytics_map['WMF-Last-Access'], 'dd-MMM-yyyy'),
           NULL) AS last_access,
        x_analytics_map['nocookies'] AS nocookies,
        actor_signature
    FROM ${pageview_actor_table}
    WHERE x_analytics_map IS NOT NULL
      AND agent_type = 'user'
      AND is_pageview = TRUE
      AND year = ${year}
      AND month = ${month}
),

-- Only keeping clients having 1 event without cookies
-- (fresh sessions are not counted when calculating the underestimate via last_access_cookie setting)
fresh_sessions_aggregated AS (
    SELECT
        domain,
        access_method,
        country_code,
        COUNT(1) AS uniques_offset
    FROM (
        SELECT
            actor_signature,
            domain,
            access_method,
            country_code,
            SUM(CASE WHEN (nocookies IS NOT NULL) THEN 1 ELSE 0 END)
        FROM
            last_access_dates
        GROUP BY
            actor_signature,
            domain,
            access_method,
            country_code
        -- Only keeping clients having done 1 event without cookies
        HAVING SUM(CASE WHEN (nocookies IS NOT NULL) THEN 1 ELSE 0 END) = 1
        ) fresh_sessions
    GROUP BY
        domain,
        access_method,
        country_code
),

-- Aggregate last access uniques before joining with fresh sessions.
-- Otherwise, Spark has trouble joining the data because it's skewed.
-- Also, calculate uniques_underestimate.
last_access_uniques_aggregated AS (
    SELECT
        domain,
        access_method,
        country_code,
        SUM(CASE
            -- domain set, last-access not set and client accept cookies --> first visit, count
            WHEN (domain IS NOT NULL AND last_access IS NULL AND nocookies is NULL) THEN 1
            -- last-access set and its date is before today --> First visit today, count
            WHEN ((last_access IS NOT NULL)
                AND (last_access < unix_timestamp(CONCAT('${year}-', LPAD('${month}', 2, '0'), '-01'), 'yyyy-MM-dd'))) THEN 1
            -- Other cases, don't count
            ELSE 0
        END) AS uniques_underestimate
    FROM
        last_access_dates
    GROUP BY
        domain,
        access_method,
        country_code
)

INSERT INTO ${unique_devices_destination_table}

-- Join last_access_uniques_aggregated with fresh_sessions_aggregated
-- to calculate uniques_estimate = uniques_underestimate + uniques_offset.
SELECT /*+ COALESCE(${coalesce_partitions}) */
    COALESCE(last_access_uniques.domain, fresh_sessions.domain) AS domain,
    COALESCE(last_access_uniques.access_method, fresh_sessions.access_method) AS access_method,
    COALESCE(countries.name, '(missing country name)') AS country,
    last_access_uniques.country_code AS country_code,
    COALESCE(last_access_uniques.uniques_underestimate, 0) AS uniques_underestimate,
    COALESCE(fresh_sessions.uniques_offset, 0) AS uniques_offset,
    COALESCE(last_access_uniques.uniques_underestimate, 0) + COALESCE(fresh_sessions.uniques_offset, 0) AS uniques_estimate,
    TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), '01'), 'yyyy-MM-dd') as day
FROM
    last_access_uniques_aggregated AS last_access_uniques
    LEFT JOIN ${countries_table} AS countries
        ON countries.iso_code = last_access_uniques.country_code
    -- Outer join to keep every row from both table
    FULL OUTER JOIN fresh_sessions_aggregated AS fresh_sessions
        -- No need to add country here as country_code matches
        ON last_access_uniques.domain = fresh_sessions.domain
          AND last_access_uniques.access_method = fresh_sessions.access_method
          AND last_access_uniques.country_code = fresh_sessions.country_code
WHERE COALESCE(last_access_uniques.uniques_underestimate, 0) + COALESCE(fresh_sessions.uniques_offset, 0) > 0
ORDER BY day, domain, access_method, country_code
;
