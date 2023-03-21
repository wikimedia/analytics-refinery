-- Inserts hourly mediacounts into a separate table
--
-- Usage:
-- spark3-sql  -f insert_hourly_mediacounts.hql                                                                 \
--             -d source_table=wmf_raw.webrequest                                                               \
--             -d destination_table=wmf.mediacounts                                                             \
--             -d year=2021                                                                                     \
--             -d month=2                                                                                       \
--             -d day=9                                                                                         \
--             -d hour=6                                                                                        \
--             -d coalesce_partitions=4                                                                       \
--             -d refinery_hive_jar=hdfs://analytics-hadoop/some/path/to/refinery-hive-0.2.1.jar'             \

ADD JAR ${refinery_hive_jar};
CREATE TEMPORARY FUNCTION parse_media_file_url AS 'org.wikimedia.analytics.refinery.hive.GetMediaFilePropertiesUDF';
CREATE TEMPORARY FUNCTION classify_referer AS 'org.wikimedia.analytics.refinery.hive.GetRefererTypeUDF';

WITH aggregated_query as (SELECT response_size,
                                 parse_media_file_url(uri_path) parsed_url,
                                 classify_referer(referer)      classified_referer
                          FROM ${source_table}
                          WHERE webrequest_source = 'upload'
                            AND year = ${year}
                            AND month = ${month}
                            AND day = ${day}
                            AND hour = ${hour}
                            AND uri_host = 'upload.wikimedia.org'
                            AND (http_status = 200 -- No 304 per RFC discussion
                                  OR (http_status = 206
                                  AND SUBSTR(`range`, 1, 8) = 'bytes=0-'
                                  AND `range` != 'bytes=0-0')))
INSERT OVERWRITE TABLE ${destination_table}
PARTITION(year = ${year},
          month = ${month},
          day = ${day},
          hour = ${hour})
SELECT /*+ COALESCE(${coalesce_partitions}) */
    parsed_url.base_name                                                                     base_name,
    SUM(response_size)                                                                       total_response_size,
    SUM(1)                                                                                   total,
    SUM(IF(parsed_url.is_original, 1, 0))                                                    original,
    SUM(IF(parsed_url.is_transcoded_to_audio, 1, 0))                                         transcoded_audio,
    SUM(IF(parsed_url.is_transcoded_to_image, 1, 0))                                         transcoded_image,
    SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 0 AND 199, 1, 0))  transcoded_image_0_199,
    SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 200 AND 399, 1,
           0))                                                                               transcoded_image_200_399,
    SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 400 AND 599, 1,
           0))                                                                               transcoded_image_400_599,
    SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 600 AND 799, 1,
           0))                                                                               transcoded_image_600_799,
    SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width BETWEEN 800 AND 999, 1,
           0))                                                                               transcoded_image_800_999,
    SUM(IF(parsed_url.is_transcoded_to_image AND parsed_url.width >= 1000, 1, 0))            transcoded_image_1000,
    SUM(IF(parsed_url.is_transcoded_to_movie, 1, 0))                                         transcoded_movie,
    SUM(IF(parsed_url.is_transcoded_to_movie AND parsed_url.height BETWEEN 0 AND 239, 1, 0)) transcoded_movie_0_239,
    SUM(IF(parsed_url.is_transcoded_to_movie AND parsed_url.height BETWEEN 240 AND 479, 1,
           0))                                                                               transcoded_movie_240_479,
    SUM(IF(parsed_url.is_transcoded_to_movie AND parsed_url.height >= 480, 1, 0))            transcoded_movie_480,
    SUM(IF(classified_referer = 'internal', 1, 0))                                           referer_internal,
    SUM(IF(classified_referer LIKE 'external%', 1, 0))                                       referer_external,
    SUM(IF(classified_referer = 'unknown' OR classified_referer = 'none', 1, 0))             referer_unknown
FROM aggregated_query
WHERE parsed_url.base_name IS NOT NULL
GROUP BY parsed_url.base_name;