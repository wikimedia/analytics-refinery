-- Parameters:
--     source_table      -- Fully qualified table name to check against
--                          the whitelist table
--     whitelist_table   -- Fully qualified table name for the whitelist to be
--                          used as filter
--     destination_table -- Fully qualified table name to fill in the values not
--                          in the whitelist
--     year              -- year of partition to whitelist-check.
--     month             -- month of partition to whitelist-check
--     day               -- day of partition to whitelist-check
--     hour              -- hour of partition to whitelist-check
--
-- Usage:
--     hive -f pageview_whitelist_check.hql                       \
--         -d source_table=wmf.pageview_hourly                    \
--         -d whitelist_table=wmf.pageview_whitelist              \
--         -d destination_table=wmf.pageview_unexpected_values    \
--         -d year=2015                                           \
--         -d month=5                                             \
--         -d day=1                                               \
--         -d hour=1
--

-- Hard-limiting number of reducer to force a single file in the
-- target directory.
SET mapred.reduce.tasks=1;

-- Restrict to needed fields for the given hour
WITH pageview_hour AS (
    SELECT
        project,
        access_method,
        agent_type,
        SUM(view_count) as view_count
    FROM ${source_table}
    WHERE year= ${year}
        AND month = ${month}
        AND day = ${day}
        AND hour = ${hour}
    GROUP BY
        project,
        access_method,
        agent_type
),

-- Get distinct values with field names using previously aggregated data
distinct_values AS (

    -- distinct project
    SELECT
        'project' AS field_name,
        project as value,
        SUM(view_count) as view_count
    FROM pageview_hour
    GROUP BY project

    UNION ALL

    -- distinct access_method
    SELECT
        'access_method' AS field_name,
        access_method as value,
        SUM(view_count) as view_count
    FROM pageview_hour
    GROUP BY access_method

    UNION ALL

    -- distinct agent_type
    SELECT
        'agent_type' AS field_name,
        agent_type as value,
        SUM(view_count) as view_count
    FROM pageview_hour
    GROUP BY agent_type

)

-- Write in destination table partition the data that didn't match the join
INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
SELECT
    distinct_values.field_name,
    distinct_values.value,
    distinct_values.view_count
FROM distinct_values
    LEFT JOIN ${whitelist_table} AS whitelist ON (
        distinct_values.field_name = whitelist.field_name
        AND distinct_values.value = whitelist.authorized_value)
WHERE
    -- Keep only non-matching values
    whitelist.field_name IS NULL;
