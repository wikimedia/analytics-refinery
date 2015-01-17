SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
--^ To work around HIVE-3296, we have SETs before any comments

-- Generates an hourly pagecounts-all-sites pagecounts file into HDFS
--
-- Parameters:
--     destination_directory -- Directory in HDFS where to store the generated
--                          data in.
--     source_table      -- table containing hourly aggregated
--                          pagecounts-all-sites data
--     year              -- year of the to-be-generated hour
--     month             -- month of the to-be-generated hour
--     day               -- day of the to-be-generated hour
--     hour              -- hour of the to-be-generated-hour
--     extra_filter      -- additional condition by which to filter the
--                          selected rows. This parameter allows to filter
--                          pagecount-all-sites results further down (e.g.: it
--                          gets used to generate pagecounts-raw by stripping
--                          unneeded rows from pagecounts-all-sites results)
--
--
-- Usage:
--     hive -f archive_pagecounts.hql               \
--         -d destination_directory=/tmp/foo        \
--         -d source_table=wmf.pagecounts_all_sites \
--         -d year=2014                             \
--         -d month=4                               \
--         -d day=1                                 \
--         -d hour=0
--

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    SELECT CONCAT_WS(
       " ",
       qualifier,
       page_title,
       cast(count_views AS string),
       cast(total_response_size AS string)
    ) line FROM ${source_table}
    WHERE year=${year}
        AND month=${month}
        AND day=${day}
        AND hour=${hour}
        ${extra_filter}
    ORDER BY line
    LIMIT 100000000;
