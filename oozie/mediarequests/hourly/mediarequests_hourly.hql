SET parquet.compression              = SNAPPY;
SET hive.enforce.bucketing           = true;
SET mapreduce.job.reduces            = 64;

ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;
CREATE TEMPORARY FUNCTION parse_media_file_url AS 'org.wikimedia.analytics.refinery.hive.GetMediaFilePropertiesUDF';
CREATE TEMPORARY FUNCTION classify_referer AS 'org.wikimedia.analytics.refinery.hive.GetRefererTypeUDF';
CREATE TEMPORARY FUNCTION referer_wiki AS 'org.wikimedia.analytics.refinery.hive.GetRefererWikiUDF';

WITH upload_webrequests AS (
    SELECT
        response_size,
        parse_media_file_url(uri_path) parsed_url,
        referer_wiki(referer) referer_wiki,
        classify_referer(referer) classified_referer,
        agent_type
    FROM ${source_table}
        WHERE webrequest_source='upload'
            AND year = ${year}
            AND month = ${month}
            AND day = ${day}
            AND hour = ${hour}
            AND uri_host = 'upload.wikimedia.org'
            AND (
                http_status = 200 -- No 304 per RFC discussion
                OR (http_status = 206
                    AND SUBSTR(range, 1, 8) = 'bytes=0-'
                    AND range != 'bytes=0-0'
                )
            )
)
INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})
    SELECT
        parsed_url.base_name base_name,
        parsed_url.media_classification media_classification,
        parsed_url.file_type file_type,
        SUM(response_size) total_bytes,
        COUNT(*) request_count,
        parsed_url.transcoding transcoding,
        agent_type,
        IF(classified_referer = 'internal', COALESCE(referer_wiki, 'internal'), COALESCE(classified_referer, 'unknown')) referer,
        CONCAT(
            LPAD(${year}, 4, "0"), '-',
            LPAD(${month}, 2, "0"), '-',
            LPAD(${day}, 2, "0"), 'T',
            LPAD(${hour}, 2, "0"),
            ':00:00Z'
        ) dt
    FROM upload_webrequests
    WHERE parsed_url.base_name IS NOT NULL
    GROUP BY
        parsed_url.base_name,
        parsed_url.media_classification,
        parsed_url.file_type,
        IF(classified_referer = 'internal', COALESCE(referer_wiki, 'internal'), COALESCE(classified_referer, 'unknown')),
        parsed_url.transcoding,
        agent_type
;
