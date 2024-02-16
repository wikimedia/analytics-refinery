-- Parameters:
--     source_table         -- Fully qualified table name to compute the
--                             aggregation for.
--     destination_table    -- Fully qualified table name to fill in
--                             aggregated values.
--     coalesce_partitions  -- Number of partitions to write
--     target_year          -- Year of partition to compute aggregation
--                             for.
--     target_month         -- Month of partition to compute aggregation
--                             for.
--     target_day           -- Day of partition to compute aggregation
--                             for.
--
-- spark3-sql -f cx_abuse_filter_daily.hql                                           \
--            -d source_table=event.contenttranslationabusefilter       \
--            -d destination_table=wmf_product.cx_abuse_filter_daily    \
--            -d coalesce_partitions=1                                  \
--            -d target_year=2024                                       \
--            -d target_month=2                                         \
--            -d target_day=19

-- Delete existing data for the period to prevent duplication of data in case of recomputation
DELETE FROM ${destination_table}
WHERE
    day = CAST(CONCAT(
        LPAD(${target_year}, 4, '0'), '-',
        LPAD(${target_month}, 2, '0'), '-',
        LPAD(${target_day}, 2, '0'), ' '
        ) AS DATE)
;

-- Compute data for the period
INSERT INTO TABLE ${destination_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
    wiki as project,
    abuse_filter_id,
    COUNT(wiki) as trigger_count,
    CAST(CONCAT(
        LPAD(${target_year}, 4, '0'), '-',
        LPAD(${target_month}, 2, '0'), '-',
        LPAD(${target_day}, 2, '0'), ' '
        ) AS DATE)
    AS day
FROM (
    SELECT DISTINCT
        CONCAT(
            event.filterId,
            event.sourceLanguage,
            event.sourceTitle,
            event.targetLanguage,
            event.token
        ) AS session,
        event.filterId as abuse_filter_id,
        wiki
    FROM
        ${source_table}
    WHERE
        year  = ${target_year} AND
        month = ${target_month} AND
        day   = ${target_day}
) AS source
GROUP BY
    abuse_filter_id,
    wiki
ORDER BY
    project,
    abuse_filter_id
;
