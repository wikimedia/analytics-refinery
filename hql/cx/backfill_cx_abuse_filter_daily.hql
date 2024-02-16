-- Parameters:
--     source_table         -- Fully qualified table name to backfill
--                             values from.
--     destination_table    -- Fully qualified table name to fill in
--                             backfilled values.
--     coalesce_partitions  -- Number of partitions to write
--
-- Usage:
-- spark3-sql -f backfill_cx_daily.hql                                  \
--            -d source_table=amire80.cx_abuse_filter_daily             \
--            -d destination_table=wmf_product.cx_abuse_filter_daily    \
--            -d coalesce_partitions=1


-- Compute data for the period
INSERT INTO TABLE ${destination_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
    project,
    abuse_filter_id,
    count,
    CAST(CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-',
        LPAD(day, 2, '0'), ' '
        ) AS DATE) AS day
FROM ${source_table}
ORDER BY
    day,
    project,
    abuse_filter_id
;
