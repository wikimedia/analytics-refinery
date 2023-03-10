-- Parameters:
--     source_table      -- Fully qualified table name to check against
--                          the allowlist table
--     allowlist_table   -- Fully qualified table name for the allowlist to be
--                          used as filter
--     destination_table -- Fully qualified table name to fill in the values not
--                          in the allowlist
--     year              -- year of partition to allowlist-check.
--     month             -- month of partition to allowlist-check
--     day               -- day of partition to allowlist-check
--     coalesce_partitions   -- the number of final partitions.
--
-- Usage:
--     spark3-sql --master yarn -f pageview_allowlist_check.hql   \
--         -d source_table=wmf.projectview_hourly                 \
--         -d allowlist_table=wmf.pageview_allowlist              \
--         -d destination_table=wmf.pageview_unexpected_values    \
--         -d year=2023                                           \
--         -d month=2                                             \
--         -d day=7                                               \
--         -d coalesce_partitions=1
--

-- Restrict to needed fields for the given day
WITH projectview_day AS (
    SELECT
        project,
        access_method,
        agent_type,
        SUM(view_count) as view_count
    FROM ${source_table}
    WHERE year= ${year}
        AND month = ${month}
        AND day = ${day}
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
    FROM projectview_day
    GROUP BY project

    UNION ALL

    -- distinct access_method
    SELECT
        'access_method' AS field_name,
        access_method as value,
        SUM(view_count) as view_count
    FROM projectview_day
    GROUP BY access_method

    UNION ALL

    -- distinct agent_type
    SELECT
        'agent_type' AS field_name,
        agent_type as value,
        SUM(view_count) as view_count
    FROM projectview_day
    GROUP BY agent_type

)

-- Write in destination table partition the data that didn't match the join
INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day})
 SELECT /*+ COALESCE(${coalesce_partitions}) */
    distinct_values.field_name,
    distinct_values.value,
    distinct_values.view_count
FROM distinct_values
    LEFT JOIN ${allowlist_table} AS allowlist ON (
        distinct_values.field_name = allowlist.field_name
        AND distinct_values.value = allowlist.authorized_value)
WHERE
    -- Keep only non-matching values
    allowlist.field_name IS NULL;
