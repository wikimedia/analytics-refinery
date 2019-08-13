ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-0.0.96.jar;
CREATE TEMPORARY FUNCTION parse_media_file_url AS 'org.wikimedia.analytics.refinery.hive.GetMediaFilePropertiesUDF';

SELECT
    base_name,
    file_classification,
    file_type,
    total_response_size,
    request_count,
    transcoding,
    NULL as agent_type,
    referer
FROM (SELECT * FROM fdans.mediacounts WHERE year = 2019 limit 3) t1
LATERAL VIEW explode(map(
    'original', original,
    'audio', transcoded_audio,
    'image', transcoded_image,
    'image_0_199', transcoded_image_0_199,
    'image_200_399', transcoded_image_200_399,
    'image_400_599', transcoded_image_400_599,
    'image_600_799', transcoded_image_600_799,
    'image_800_999', transcoded_image_800_999,
    'image_1000', transcoded_image_1000,
    'movie', transcoded_movie,
    'movie_0_239', transcoded_movie_0_239,
    'movie_240_479', transcoded_movie_240_479,
    'movie_480', transcoded_movie_480
)) transcodes AS transcoding, total
WHERE total > 0;