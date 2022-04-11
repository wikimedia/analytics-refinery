-- Creates table for hourly mediarequest dataset
--
--
-- Usage
--     hive -f create_mediarequest_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `mediarequest` (
    `base_name`                string COMMENT 'Base name of media file',
    `media_classification`     string COMMENT 'General classification of media (image, video, audio, data, document or other)',
    `file_type`                string COMMENT 'Extension or suffix of the file (e.g. jpg, wav, pdf)',
    `total_bytes`              bigint COMMENT 'Total number of bytes',
    `request_count`            bigint COMMENT 'Total number of requests',
    `transcoding`              string COMMENT 'Transcoding that the file was requested with, e.g. resized photo or image preview of a video',
    `agent_type`               string COMMENT 'Agent accessing the media files, can be spider or user',
    `referer`                  string COMMENT 'Wiki project that the request was refered from. If project is not available, it will be either internal, external, or unknown',
    `dt`                       string COMMENT 'UTC timestamp in ISO 8601 format (e.g. 2019-08-27T14:00:00Z)'
)
PARTITIONED BY (
    `year`                int    COMMENT 'Unpadded year',
    `month`               int    COMMENT 'Unpadded month',
    `day`                 int    COMMENT 'Unpadded day',
    `hour`                int    COMMENT 'Unpadded hour'
)
STORED AS PARQUETFILE
LOCATION '/wmf/data/wmf/mediarequest'
;
