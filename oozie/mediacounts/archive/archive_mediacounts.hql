SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec;
SET parquet.compression=SNAPPY;
--^ To work around HIVE-3296, we have SETs before any comments

-- Archives mediacounts data for upload webrequest_source
--
-- Parameters:
--     destination_directory -- Directory in HDFS where to store the generated
--                              data in.
--     source_table          -- table containing pre-aggregated hourly
--                              mediacounts data
--     year                  -- year of the date to archive for
--     month                 -- month of the date to archive for
--     day                   -- day of the date to archive for
--
--
-- Usage:
--     hive -f archive_mediacounts.hql       \
--         -d destination_directory=/wmf/tmp/analytics/foo \
--         -d source_table=wmf.mediacounts   \
--         -d year=2014                      \
--         -d month=4                        \
--         -d day=1
--
--
--

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    SELECT
        -------------------------------------------------------------
        --                                                         --
        -- If you change the format/meaning of this select, please --
        -- increase the format_version in workflow.xml             --
        --                                                         --
        -------------------------------------------------------------
        CONCAT_WS("	",
            base_name,
            CAST(total_response_size      AS string),
            CAST(total                    AS string),
            CAST(original                 AS string),
            CAST(transcoded_audio         AS string),
            "-", -- reserved for future use
            "-", -- reserved for future use
            CAST(transcoded_image         AS string),
            CAST(transcoded_image_0_199   AS string),
            CAST(transcoded_image_200_399 AS string),
            CAST(transcoded_image_400_599 AS string),
            CAST(transcoded_image_600_799 AS string),
            CAST(transcoded_image_800_999 AS string),
            CAST(transcoded_image_1000    AS string),
            "-", -- reserved for future use
            "-", -- reserved for future use
            CAST(transcoded_movie         AS string),
            CAST(transcoded_movie_0_239   AS string),
            CAST(transcoded_movie_240_479 AS string),
            CAST(transcoded_movie_480     AS string),
            "-", -- reserved for future use
            "-", -- reserved for future use
            CAST(referer_internal         AS string),
            CAST(referer_external         AS string),
            CAST(referer_unknown          AS string)
        ) line
    FROM (
        SELECT
            base_name,
            SUM(total_response_size) total_response_size,
            SUM(total) total,
            SUM(original) original,
            SUM(transcoded_audio) transcoded_audio,
            SUM(transcoded_image) transcoded_image,
            SUM(transcoded_image_0_199) transcoded_image_0_199,
            SUM(transcoded_image_200_399) transcoded_image_200_399,
            SUM(transcoded_image_400_599) transcoded_image_400_599,
            SUM(transcoded_image_600_799) transcoded_image_600_799,
            SUM(transcoded_image_800_999) transcoded_image_800_999,
            SUM(transcoded_image_1000) transcoded_image_1000,
            SUM(transcoded_movie) transcoded_movie,
            SUM(transcoded_movie_0_239) transcoded_movie_0_239,
            SUM(transcoded_movie_240_479) transcoded_movie_240_479,
            SUM(transcoded_movie_480) transcoded_movie_480,
            SUM(referer_internal) referer_internal,
            SUM(referer_external) referer_external,
            SUM(referer_unknown) referer_unknown
        FROM ${source_table}
        WHERE year=${year}
            AND month=${month}
            AND day=${day}
        GROUP BY base_name
        ORDER BY base_name
        -- as of 2014-10-01, we're seeing 16M lines per day. So 1000M should be safe for now.
        LIMIT 1000000000
    ) daily
;