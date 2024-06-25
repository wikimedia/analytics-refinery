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
--     projectview_source       -- Table containing hourly projectviews, for quicker overall aggregates.
--     pageview_source          -- Table containing hourly pageviews.
--     year                     -- Year of the date to update.
--     month                    -- Month of the date to update.
--     day                      -- Day of the date to update.
--     privacy_threshold        -- The least number of views per group we want to output to protect privacy.
--     output_threshold         -- The least number of views per group we want to output to keep the output size useful.
--                              --   a threshold of 15k per day is exactly 10 requests per minute,
--                              --   see T342267#9913721 for the discussion around this
--     os_family_unknown        -- Default unknown value for os family.
--     os_major_unknown         -- Default unknown value for os major.
--     browser_family_unknown   -- Default unknown value for browser family.
--     browser_major_unknown    -- Default unknown value for browser major.
--     destination_table        -- Table where to write the report.
--
-- Usage:
--     NOTE: with these settings, a day executes without memory issues and finishes in about 70 seconds

--     spark3-sql -f browser_general_iceberg.hql                    \
--          --master yarn --executor-cores 2 --executor-memory 1G   \
--          --conf spark.dynamicAllocation.maxExecutors=64          \
--          --conf spark.executor.memoryOverhead=1G                 \
--          -d coalesce_partitions=1                                \
--          -d pageview_source=wmf.pageview_hourly                  \
--          -d projectview_source=wmf.projectview_hourly            \
--          -d year=2024                                            \
--          -d month=6                                              \
--          -d day=10                                               \
--          -d privacy_threshold=250                                \
--          -d output_threshold=15000                               \
--          -d os_family_unknown=Redacted                           \
--          -d os_major_unknown=Redacted                            \
--          -d browser_family_unknown=Redacted                      \
--          -d browser_major_unknown=Redacted                       \
--          -d destination_table=milimetric.browser_general_test

SET today = TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), LPAD(${day}, 2, '0')), 'yyyy-MM-dd');

-- Delete existing data for the day to prevent duplication of data in case of recomputation
DELETE FROM ${destination_table} WHERE day = ${today};

-- the output here is generally fewer than 10k rows, should fit perfectly well in memory
CACHE TABLE privacy_safe_bucket AS (
    -- Per discussion in T342267, results in this set are ok to share as-is from a privacy perspective,
    -- so we base the rest of our rollups on this.
    -- NOTE: data publication guidelines:
    --   https://foundation.wikimedia.org/wiki/Legal:Data_publication_guidelines#Threshold_table
    SELECT
        access_method,
        user_agent_map['os_family'] os_family,
        user_agent_map['os_major'] os_major,
        user_agent_map['browser_family'] browser_family,
        user_agent_map['browser_major'] browser_major,
        SUM(view_count) view_count
    FROM
        ${pageview_source}
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
        user_agent_map['browser_major']
    HAVING
        SUM(view_count) > ${privacy_threshold}
);

WITH
    -- The overall total for this period and desired filters (faster in projectview)
    overall AS (
        SELECT
            access_method,
            SUM(view_count) total
        FROM
            ${projectview_source}
        WHERE
            year = ${year}
            AND month = ${month}
            AND day = ${day}
            AND agent_type = 'user'
            AND access_method IN ('desktop', 'mobile web')
        GROUP BY
            access_method
    ),

    /**
     * The following four CTEs roll up privacy_safe_bucket in different ways for output
     * Each rollup uses fewer columns than the previous one, so it has less specific results
     * And each rollup anti-joins to previous rollups so that it only groups what hasn't been output yet
     */
    output_without_rollup AS (
        SELECT
            access_method,
            os_family,
            os_major,
            browser_family,
            browser_major,
            view_count
        FROM
            privacy_safe_bucket
        WHERE
            view_count > ${output_threshold}
    ),

    output_rollup_os_major AS (
        SELECT
            access_method,
            os_family,
            '${os_major_unknown}' os_major,
            browser_family,
            browser_major,
            SUM(view_count) view_count
        FROM
            privacy_safe_bucket
            ANTI JOIN output_without_rollup
                USING (access_method, os_family, os_major, browser_family, browser_major)
        GROUP BY
            access_method,
            os_family,
            browser_family,
            browser_major
        HAVING
            SUM(view_count) > ${output_threshold}
    ),

    output_rollup_os_major_browser_major AS (
        SELECT
            access_method,
            os_family,
            '${os_major_unknown}' os_major,
            browser_family,
            '${browser_major_unknown}' browser_major,
            SUM(view_count) view_count
        FROM
            privacy_safe_bucket
            ANTI JOIN output_without_rollup
                USING (access_method, os_family, os_major, browser_family, browser_major)
            ANTI JOIN output_rollup_os_major
                USING (access_method, os_family, browser_family, browser_major)
        GROUP BY
            access_method,
            os_family,
            browser_family
        HAVING
            SUM(view_count) > ${output_threshold}
    ),

    output_rollup_os_family_os_major_browser_major AS (
        SELECT
            access_method,
            '${os_family_unknown}' os_family,
            '${os_major_unknown}' os_major,
            browser_family,
            '${browser_major_unknown}' browser_major,
            SUM(view_count) view_count
        FROM
            privacy_safe_bucket
            ANTI JOIN output_without_rollup
                USING (access_method, os_family, os_major, browser_family, browser_major)
            ANTI JOIN output_rollup_os_major
                USING (access_method, os_family, browser_family, browser_major)
            ANTI JOIN output_rollup_os_major_browser_major
                USING (access_method, os_family, browser_family)
        GROUP BY
            access_method,
            browser_family
        HAVING
            SUM(view_count) > ${output_threshold}
    ),

    detailed_output AS (
        SELECT * FROM output_without_rollup
        UNION ALL
        SELECT * FROM output_rollup_os_major
        UNION ALL
        SELECT * FROM output_rollup_os_major_browser_major
        UNION ALL
        SELECT * FROM output_rollup_os_family_os_major_browser_major
    ),

    total_detailed_output AS (
        SELECT
            access_method,
            SUM(view_count) total
        FROM
            detailed_output
        GROUP BY
            access_method
    )

-- Writes the data for period into the destination table.
INSERT INTO ${destination_table}

-- to have a single file per day in the iceberg data folder
SELECT  /*+ COALESCE(${coalesce_partitions}) */ *
  FROM (

    SELECT
        access_method,
        os_family,
        os_major,
        browser_family,
        browser_major,
        view_count,
        ${today} day
    FROM detailed_output

    UNION ALL

    SELECT
        overall.access_method,
        '${os_family_unknown}' os_family,
        '${os_major_unknown}' os_major,
        '${browser_family_unknown}' browser_family,
        '${browser_major_unknown}' browser_major,
        overall.total - total_detailed_output.total AS view_count,
        ${today} day
    FROM
        overall
        INNER JOIN total_detailed_output USING (access_method)
);
