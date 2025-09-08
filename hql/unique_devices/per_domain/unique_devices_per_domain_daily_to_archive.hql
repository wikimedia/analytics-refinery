-- Aggregate daily per-domain unique devices on domain and
-- keep only domains having more than 1000 uniques daily.
--
-- Parameters:
--     source_table           -- Table containing source data
--     destination_directory  -- Table where to write newly computed data
--     year                   -- year of the to-be-generated
--     month                  -- month of the to-be-generated
--     day                    -- day of the to-be-generated
--
-- Usage:
--     spark-sql -f unique_devices_per_domain_daily_to_archive.hql \
--         -d source_table=wmf.unique_devices_per_domain_daily \
--         -d destination_directory=/wmf/tmp/analytics/unique_devices_per_domain_daily \
--         -d year=2022 \
--         -d month=9 \
--         -d day=1


INSERT OVERWRITE DIRECTORY "${destination_directory}"
    USING CSV OPTIONS ('sep' = '\t', 'compression' = 'gzip')

    -- Coalesce to 1 to generate just 1 output file.
    SELECT /*+ COALESCE(1) */
        domain,
        access_method,
        SUM(uniques_underestimate) AS uniques_underestimate,
        SUM(uniques_offset) AS uniques_offset,
        SUM(uniques_estimate) AS uniques_estimate
    FROM ${source_table}
    WHERE day = TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), LPAD(${day}, 2, '0')), 'yyyy-MM-dd');
    GROUP BY
        domain,
        access_method
    HAVING
        SUM(uniques_estimate) >= 1000
    ORDER BY
        uniques_estimate DESC
;
