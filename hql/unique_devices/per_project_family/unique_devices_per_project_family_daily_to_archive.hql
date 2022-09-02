-- Aggregate unique devices per-project-family daily by project_family and remove wikimedia.
--
-- Parameters:
--     source_table           -- Table containing source data
--     destination_directory  -- Table where to write newly computed data
--     year                   -- year of the to-be-generated
--     month                  -- month of the to-be-generated
--     day                    -- day of the to-be-generated
--
-- Usage:
--     spark-sql -f unique_devices_per_project_family_daily_to_archive.hql \
--         -d source_table=wmf.unique_devices_per_project_family_daily \
--         -d destination_directory=/wmf/tmp/analytics/unique_devices/per_project_family \
--         -d year=2022 \
--         -d month=9 \
--         -d day=1


INSERT OVERWRITE DIRECTORY "${destination_directory}"
    USING CSV OPTIONS ('sep' = '\t', 'compression' = 'gzip')

    -- Coalesce to 1 to generate just 1 output file.
    SELECT /*+ COALESCE(1) */
        project_family,
        SUM(uniques_underestimate) AS uniques_underestimate,
        SUM(uniques_offset) AS uniques_offset,
        SUM(uniques_estimate) AS uniques_estimate
    FROM ${source_table}
    WHERE year=${year}
        AND month=${month}
        AND day=${day}
        AND project_family != 'wikimedia'
    GROUP BY
        project_family
    ORDER BY
        uniques_estimate DESC
;
