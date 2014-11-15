-- Inserts hourly mediacounts into a separate table
--
-- Usage:
--     hive -f insert_hourly_mediacounts.hql \
--         -d source_table=wmf_raw.webrequest \
--         -d destination_table=wmf.mediacounts \
--         -d year=2014 \
--         -d month=9 \
--         -d day=15 \
--         -d hour=20
--

SET parquet.compression              = SNAPPY;
SET hive.enforce.bucketing           = true;
-- mapreduce.job.reduces should not be necessary to
-- specify since we set hive.enforce.bucketing=true.
-- However, without this set, only one reduce task is
-- launched, so we set it manually.  This needs
-- to be the same as the number of buckets the
-- table is clustered by.
SET mapreduce.job.reduces            = 64;

ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-0.0.7.jar;
CREATE TEMPORARY FUNCTION parse_media_file_url AS 'org.wikimedia.analytics.refinery.hive.MediaFileUrlParserUDF';
CREATE TEMPORARY FUNCTION classify_referer AS 'org.wikimedia.analytics.refinery.hive.RefererClassifierUDF';


INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})
    SELECT
        parsed_url.base_name base_name,
        SUM(response_size) total_response_size,
        SUM(1) total,
        SUM(IF(parsed_url.is_original, 1, 0)) original,
        SUM(IF(parsed_url.is_transcoded_to_audio, 1, 0)) transcoded_audio,
        SUM(IF(parsed_url.is_transcoded_to_image, 1, 0)) transcoded_image,
        SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 0 AND 199, 1, 0)) transcoded_image_0_199,
        SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 200 AND 399, 1, 0)) transcoded_image_200_399,
        SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 400 AND 599, 1, 0)) transcoded_image_400_599,
        SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 600 AND 799, 1, 0)) transcoded_image_600_799,
        SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 800 AND 999, 1, 0)) transcoded_image_800_999,
        SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width >= 1000, 1, 0)) transcoded_image_1000,
        SUM(IF(parsed_url.is_transcoded_to_movie, 1, 0)) transcoded_movie,
        SUM(IF(parsed_url.is_transcoded_to_movie AND parsed_url.height BETWEEN 0 AND 239, 1, 0)) transcoded_movie_0_239,
        SUM(IF(parsed_url.is_transcoded_to_movie AND parsed_url.height BETWEEN 240 AND 479, 1, 0)) transcoded_movie_240_479,
        SUM(IF(parsed_url.is_transcoded_to_movie AND parsed_url.height >= 480, 1, 0)) transcoded_movie_480,
        SUM(IF(classified_refererer.is_internal, 1, 0)) referer_internal,
        SUM(IF(classified_refererer.is_external, 1, 0)) referer_external,
        SUM(IF(classified_refererer.is_unknown, 1, 0)) referer_unknown
    FROM (
        SELECT
            response_size,
            parse_media_file_url(uri_path) parsed_url,
            classify_referer(referer) classified_refererer
        FROM ${source_table}
            WHERE webrequest_source='upload'
                AND year=${year}
                AND month=${month}
                AND day=${day}
                AND hour=${hour}
                AND uri_host = 'upload.wikimedia.org'
                AND (
                    http_status = 200 -- No 304 per RFC discussion
                    OR (http_status=206
                        AND SUBSTR(range, 1, 8) = 'bytes=0-'
                        AND range != 'bytes=0-0'
                    )
                )
        ) parsed
    WHERE parsed_url.base_name IS NOT NULL
    GROUP BY parsed_url.base_name
;
