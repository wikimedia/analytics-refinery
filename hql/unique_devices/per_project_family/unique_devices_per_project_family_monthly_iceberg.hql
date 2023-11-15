-- Insert into Iceberg unique devices per-project-family monthly from Hive table
--
-- Parameters:
--     unique_devices_source_table        -- Table containing source data
--     unique_devices_destination_table   -- Table where to write newly computed data
--     year                               -- year of the to-be-generated
--     month                              -- month of the to-be-generated
--     coalesce_partitions                -- Number of partitions to write
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
--         -f unique_devices_per_project_family_monthly_iceberg.hql \
--         -d unique_devices_source_table=wmf.unique_devices_per_project_family_monthly \
--         -d unique_devices_destination_table=mforns.unique_devices_per_project_family_monthly \
--         -d year=2022 \
--         -d month=8 \
--         -d coalesce_partitions=1

-- Delete existing data for the period to prevent duplication of data in case of recomputation
DELETE FROM ${unique_devices_destination_table}
WHERE day = TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), '01'), 'yyyy-MM-dd');

-- TODO: Replace by unique-comptation function when removing old hive table
INSERT INTO ${unique_devices_destination_table}

SELECT /*+ COALESCE(${coalesce_partitions}) */
    project_family,
    country,
    country_code,
    uniques_underestimate,
    uniques_offset,
    uniques_estimate,
    TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), '01'), 'yyyy-MM-dd') AS day
FROM
    ${unique_devices_source_table}
WHERE year = ${year}
    AND month = ${month}
ORDER BY day, project_family, country_code
;
