-- Aggregate unique devices project-wide monthly by project and remove wikimedia.
--
-- Parameters:
--     source_table           -- Table containing source data
--     destination_directory  -- Table where to write newly computed data
--     year                   -- year of the to-be-generated
--     month                  -- month of the to-be-generated
--
-- Usage:
--     hive -f unique_devices_project_wide_monthly_to_archive.hql \
--         -d source_table=wmf.unique_devices_project_wide_monthly \
--         -d destination_directory=/tmp/archive/unique_devices/project_wide \
--         -d year=2017 \
--         -d month=4


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
          project,
          cast(uniques_underestimate AS string),
          cast(uniques_offset AS string),
          cast(uniques_estimate AS string)) AS line
    FROM (
        SELECT
            project,
            SUM(uniques_underestimate) AS uniques_underestimate,
            SUM(uniques_offset) AS uniques_offset,
            SUM(uniques_estimate) AS uniques_estimate
        FROM ${source_table}
        WHERE year=${year}
            AND month=${month}
            AND project != 'wikimedia'
        GROUP BY
            project
        ORDER BY
            uniques_estimate DESC
        LIMIT 100000000
    ) uniques_transformed
;
