-- Aggregate monthly per-domain unique devices on domain and
-- keep only hosts having more than 1000 uniques monthly.
--
-- Parameters:
--     source_table           -- Table containing source data
--     destination_directory  -- Table where to write newly computed data
--     year                   -- year of the to-be-generated
--     month                  -- month of the to-be-generated
--
-- Usage:
--     spark-sql -f unique_devices_per_domain_monthly_to_archive.hql \
--         -d source_table=wmf.unique_devices_per_domain_monthly \
--         -d destination_directory=/wmf/tmp/analytics/unique_devices_per_domain_monthly \
--         -d year=2016 \
--         -d month=1


INSERT OVERWRITE DIRECTORY "${destination_directory}"
    USING CSV OPTIONS ('sep' = '\t', 'compression' = 'gzip')

    -- Coalesce to 1 to generate just 1 output file.
    SELECT /*+ COALESCE(1) */
        domain,
        SUM(uniques_underestimate) AS uniques_underestimate,
        SUM(uniques_offset) AS uniques_offset,
        SUM(uniques_estimate) AS uniques_estimate
    FROM ${source_table}
    WHERE year=${year}
        AND month=${month}
    GROUP BY
        domain
    HAVING
        SUM(uniques_estimate) >= 1000
    ORDER BY
        uniques_estimate DESC
;
