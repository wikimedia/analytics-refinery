SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
--^ To work around HIVE-3296, we have SETs before any comments

-- Generates a TSV of server errors on the mobile, and text partitions
--
-- Parameters:
--     destination_directory -- Directory in HDFS where to store the generated
--                          data in.
--     webrequest_table  -- table containing webrequests
--     year              -- year of the to-be-generated hour
--     month             -- month of the to-be-generated hour
--     day               -- day of the to-be-generated hour
--
--
-- Usage:
--     hive -f generate_5xx_tsv.hql                \
--         -d destination_directory=/tmp/foo       \
--         -d webrequest_table=wmf_raw.webrequest  \
--         -d year=2014                            \
--         -d month=4                              \
--         -d day=1
--

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    --
    -- This concatenation however means that we cannot sort by dt without also
    -- adding it to the SELECTed columns. Hence, we need to add the dt column to
    -- the select, then sort, and finally drop the dt column again. This issue
    -- buys us the subquery :-(
    SELECT
        line
    FROM (
        SELECT
            CONCAT_WS(
                "	",
                hostname,
                CAST(sequence AS string),
                dt,
                CAST(time_firstbyte AS string),
                ip,
                CONCAT_WS('/', cache_status, http_status),
                CAST(response_size AS string),
                http_method,
                CONCAT('http://', uri_host, uri_path, uri_query),
                "-",
                content_type,
                referer,
                x_forwarded_for,
                user_agent,
                accept_language,
                x_analytics
            ) line,
            dt
        FROM ${webrequest_table}
        WHERE webrequest_source IN ('text', 'mobile')
            -- TODO: Add 'bits', once it's turned on again
            -- TODO: Add 'misc', once it's available
            AND year=${year}
            AND month=${month}
            AND day=${day}
            AND http_status BETWEEN 500 AND 599
        ORDER BY dt
        LIMIT 100000000
    ) line_with_timing;
