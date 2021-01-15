-- Parameters:
--     destination_directory -- HDFS path to write output files
--     source_table          -- Fully qualified table name to compute from.
--     separator             -- Separator for values
--     year                  -- year of partition to compute from.
--     month                 -- month of partition to compute from.
--
-- Usage:
--     hive -f pageview_per_project.hql                           \
--         -d destination_directory=/wmf/tmp/analytics/pageview_per_project     \
--         -d source_table=wmf.projectview_hourly                 \
--         -d separator=\t                                        \
--         -d year=2015                                           \
--         -d month=5                                             \
--


SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    SELECT
        CONCAT_WS('${separator}',
        COALESCE(regexp_replace(project, ' ', '-'), 'all-projects'),
        COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access'),
        COALESCE(agent_type, 'all-agents'),
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), "0100"),
        CAST(SUM(view_count) AS STRING))
    FROM
        ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
    GROUP BY
        project,
        access_method,
        agent_type,
        year,
        month
    GROUPING SETS (
        (
            project,
            access_method,
            agent_type,
            year,
            month
        ),(
            project,
            agent_type,
            year,
            month
        ),(
            project,
            access_method,
            year,
            month
        ),(
            project,
            year,
            month
        ),(
            access_method,
            agent_type,
            year,
            month
        ),(
            agent_type,
            year,
            month
        ),(
            access_method,
            year,
            month
        ),(
            year,
            month
        )
    );
