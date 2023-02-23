--Archives mediacounts data for upload webrequest_source
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
--     spark-sql -f mediacounts_archive_daily.hql       \
--         -d destination_directory=/wmf/tmp/analytics/foo \
--         -d source_table=wmf.mediacounts   \
--         -d year=2022                      \
--         -d month=12                        \
--         -d day=01


INSERT OVERWRITE DIRECTORY "${destination_directory}"
    USING CSV OPTIONS ( 'sep' = '\t', 'compression' = 'bzip2')
        SELECT /*+ COALESCE(1) */
            base_name,
            SUM(total_response_size) total_response_size,
            SUM(total) total,
            SUM(original) original,
            SUM(transcoded_audio) transcoded_audio,
            "-" reserved_1, -- reserved for future use
            "-" reserved_2, -- reserved for future use
            SUM(transcoded_image) transcoded_image,
            SUM(transcoded_image_0_199) transcoded_image_0_199,
            SUM(transcoded_image_200_399) transcoded_image_200_399,
            SUM(transcoded_image_400_599) transcoded_image_400_599,
            SUM(transcoded_image_600_799) transcoded_image_600_799,
            SUM(transcoded_image_800_999) transcoded_image_800_999,
            SUM(transcoded_image_1000) transcoded_image_1000,
            "-" reserved_3, -- reserved for future use
            "-" reserved_4, -- reserved for future use
            SUM(transcoded_movie) transcoded_movie,
            SUM(transcoded_movie_0_239) transcoded_movie_0_239,
            SUM(transcoded_movie_240_479) transcoded_movie_240_479,
            SUM(transcoded_movie_480) transcoded_movie_480,
            "-" reserved_5, -- reserved for future use
            "-" reserved_6, -- reserved for future use
            SUM(referer_internal) referer_internal,
            SUM(referer_external) referer_external,
            SUM(referer_unknown) referer_unknown
        FROM ${source_table}
        WHERE year=${year}
            AND month=${month}
            AND day=${day}
        GROUP BY base_name
        ORDER BY base_name
;
