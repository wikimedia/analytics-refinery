-- Creates table for hourly mediacounts reports
--
-- NOTE:  When choosing partition field types,
-- one should take into consideration Hive's
-- insistence on storing partition values
-- as strings.  See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png
-- and
-- http://bots.wmflabs.org/~wm-bot/logs/%23wikimedia-analytics/20140721.txt
--
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediacounts_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `mediacounts` (
    `base_name`                string COMMENT 'Base name of media file',
    `total_response_size`      bigint COMMENT 'Total number of bytes',
    `total`                    bigint COMMENT 'Total #',
    `original`                 bigint COMMENT 'Sum for the raw, original file',
    `transcoded_audio`         bigint COMMENT 'Sum for audio',
    `transcoded_image`         bigint COMMENT 'Sum for image (any width)',
    `transcoded_image_0_199`   bigint COMMENT 'Sum for image (0 <= width <= 199)',
    `transcoded_image_200_399` bigint COMMENT 'Sum for image (200 <= width <= 399)',
    `transcoded_image_400_599` bigint COMMENT 'Sum for image (400 <= width <= 599)',
    `transcoded_image_600_799` bigint COMMENT 'Sum for image (600 <= width <= 799)',
    `transcoded_image_800_999` bigint COMMENT 'Sum for image (800 <= width <= 999)',
    `transcoded_image_1000`    bigint COMMENT 'Sum for image (1000 <= width)',
    `transcoded_movie`         bigint COMMENT 'Sum for movie (any height)',
    `transcoded_movie_0_239`   bigint COMMENT 'Sum for movie (0 <= height <= 239)',
    `transcoded_movie_240_479` bigint COMMENT 'Sum for movie (240 <= height <= 479)',
    `transcoded_movie_480`     bigint COMMENT 'Sum for movie (480 <= height)',
    `referer_internal`         bigint COMMENT 'Sum for WMF referers',
    `referer_external`         bigint COMMENT 'Sum for refers from non-WMF domains',
    `referer_unknown`          bigint COMMENT 'Sum for empty/invalid referers')
PARTITIONED BY (
    `year`                int    COMMENT 'Unpadded year',
    `month`               int    COMMENT 'Unpadded month',
    `day`                 int    COMMENT 'Unpadded day',
    `hour`                int    COMMENT 'Unpadded hour')
STORED AS PARQUET
LOCATION '/wmf/data/wmf/mediacounts';
