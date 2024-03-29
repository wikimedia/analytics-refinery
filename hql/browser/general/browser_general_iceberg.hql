-- Hql query for browser general data.
--
-- Updates the external table browser_general with traffic stats broken down
-- by access_method, os, browser and other dimensions. This table serves as
-- intermediate data source for various traffic reports, i.e.: mobile web
-- browser breakdown, desktop os breakdown, or desktop+mobile web os+browser
-- breakdown, etc.
--
-- Parameters:
--     coalesce_partitions      -- Number of partitions to write at query end
--     projectview_source       -- Table containing hourly projectviews.
--     pageview_source          -- Table containing hourly pageviews.
--     year                     -- Year of the date to update.
--     month                    -- Month of the date to update.
--     day                      -- Day of the date to update.
--     threshold                -- Percent where to cut the long tail.
--     os_family_unknown        -- Default unknown value for os family.
--     os_major_unknown         -- Default unknown value for os major.
--     browser_family_unknown   -- Default unknown value for browser family.
--     browser_major_unknown    -- Default unknown value for browser major.
--     destination_table        -- Table where to write the report.
--
-- Usage:
--     spark3-sql -f browser_general.hql                     \
--         -d coalesce_partitions=1                          \
--         -d projectview_source=wmf.projectview_hourly      \
--         -d pageview_source=wmf.pageview_hourly            \
--         -d year=2016                                      \
--         -d month=1                                        \
--         -d day=1                                          \
--         -d threshold=0.1                                  \
--         -d os_family_unknown=Unknown                      \
--         -d os_major_unknown=Unknown                       \
--         -d browser_family_unknown=Unknown                 \
--         -d browser_major_unknown=Unknown                  \
--         -d destination_table=wmf_traffic.browser_general

-- Delete existing data for the day to prevent duplication of data in case of recomputation
DELETE FROM ${destination_table}
WHERE day = TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), LPAD(${day}, 2, '0')), 'yyyy-MM-dd');

WITH
    total AS (
        -- This select returns 1 row with the total view counts for the whole data set.
        -- Will be used to compute percent of total for each row and collapse the long tail.
        SELECT
            SUM(view_count) as view_count_total
        FROM
            ${projectview_source}
        WHERE
            year = ${year}
            AND month = ${month}
            AND day = ${day}
            AND agent_type = 'user'
            AND access_method IN ('desktop', 'mobile web')
    ),
    stats AS (
        -- This select calculates the main stats (view count sum and percent)
        -- over the data set, grouped by os/browser family and major. The percent
        -- is calculated joining (cartesian) with the total CTE.
        SELECT
            access_method,
            user_agent_map['os_family'] AS os_family,
            user_agent_map['os_major'] AS os_major,
            user_agent_map['browser_family'] AS browser_family,
            user_agent_map['browser_major'] AS browser_major,
            SUM(view_count) AS view_count,
            SUM(view_count) * 100 / total.view_count_total AS `percent`
        FROM
            ${pageview_source}
            CROSS JOIN total
        WHERE
            year = ${year}
            AND month = ${month}
            AND day = ${day}
            AND agent_type = 'user'
            AND access_method IN ('desktop', 'mobile web')
        GROUP BY
            access_method,
            user_agent_map['os_family'],
            user_agent_map['os_major'],
            user_agent_map['browser_family'],
            user_agent_map['browser_major'],
            total.view_count_total
    ),
    anonymized_stats AS (
        -- This select anonymizes rows setting all dimension values to unknown.
        -- Note that the access_method dimension is not anonymized, because
        -- it is not privacy sensitive in this case. After that, it regroups
        -- the rows to collapse the long tail, and sorts them by view count.
        SELECT /*+ COALESCE(${coalesce_partitions}) */
            access_method,
            IF(`percent` > ${threshold}, os_family, '${os_family_unknown}') AS os_family,
            IF(`percent` > ${threshold}, os_major, '${os_major_unknown}') AS os_major,
            IF(`percent` > ${threshold}, browser_family, '${browser_family_unknown}') AS browser_family,
            IF(`percent` > ${threshold}, browser_major, '${browser_major_unknown}') AS browser_major,
            SUM(view_count) AS view_count,
            TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), LPAD(${day}, 2, '0')), 'yyyy-MM-dd') AS day
        FROM stats
        GROUP BY
            access_method,
            IF(`percent` > ${threshold}, os_family, '${os_family_unknown}'),
            IF(`percent` > ${threshold}, os_major, '${os_major_unknown}'),
            IF(`percent` > ${threshold}, browser_family, '${browser_family_unknown}'),
            IF(`percent` > ${threshold}, browser_major, '${browser_major_unknown}')
        ORDER BY day, view_count DESC
    )

-- Writes the data for period into the destination table.
INSERT INTO ${destination_table}

SELECT *
FROM anonymized_stats
;
