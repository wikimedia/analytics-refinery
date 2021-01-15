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
--     hive -f unique_devices_per_project_family_daily_to_archive.hql \
--         -d source_table=wmf.unique_devices_per_project_family_daily \
--         -d destination_directory=/wmf/tmp/analytics/unique_devices/per_project_family \
--         -d year=2017 \
--         -d month=4 \
--         -d day=1


-- Set compression codec to gzip to provide asked format
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    -- Set 0 as volume column since we don't use it.
    SELECT
        CONCAT_WS('\t',
          project_family,
          cast(uniques_underestimate AS string),
          cast(uniques_offset AS string),
          cast(uniques_estimate AS string)) AS line
    FROM (
        SELECT
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
        LIMIT 100000000
    ) uniques_transformed
;
