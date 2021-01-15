-- Extracts one day of json formatted minutely banner activity to be loaded in Druid
--
-- Usage:
--     hive -f generate_daily_druid_banner_activity.hql \
--         -d source_table=wmf.webrequest \
--         -d destination_directory=/wmf/tmp/druid/daily_json_banner_activity \
--         -d year=2016 \
--         -d month=7 \
--         -d day=10
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS tmp_banner_activity_${year}_${month}_${day};
CREATE EXTERNAL TABLE IF NOT EXISTS tmp_banner_activity_${year}_${month}_${day} (
    `dt`                             string,
    `campaign`                       string,
    `banner`                         string,
    `project`                        string,
    `uselang`                        string,
    `bucket`                         string,
    `anonymous`                      boolean,
    `status_code`                    string,
    `country`                        string,
    `country_matches_geocode`        boolean,
    `region`                         string,
    `device`                         string,
    `sample_rate`                    float,
    `request_count`                  bigint,
    `normalized_request_count`       bigint
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';


CREATE TEMPORARY MACRO uri_param_value(param_name string, col string)
    parse_url(concat('http://bla.org/woo/', col), 'QUERY', param_name);

INSERT OVERWRITE TABLE tmp_banner_activity_${year}_${month}_${day}
SELECT
    CONCAT(SUBSTRING(dt, 0, 17), '00Z') AS dt,
    uri_param_value('campaign', uri_query) AS campaign,
    uri_param_value('banner', uri_query) AS banner,
    uri_param_value('project', uri_query) AS project,
    uri_param_value('uselang', uri_query) AS uselang,
    uri_param_value('bucket', uri_query) AS bucket,
    uri_param_value('anonymous', uri_query) = 'true' AS anonymous,
    uri_param_value('statusCode', uri_query) AS status_code,
    uri_param_value('country', uri_query) AS country,
    geocoded_data['country_code'] = uri_param_value('country', uri_query) AS country_matches_geocode,
    geocoded_data['subdivision'] AS region,
    uri_param_value('device', uri_query) AS device,
    cast(uri_param_value('recordImpressionSampleRate', uri_query) AS float) AS sample_rate,
    COUNT(*) AS request_count,
    cast(COUNT(*) / cast(uri_param_value('recordImpressionSampleRate', uri_query) AS float) AS bigint) AS normalized_request_count
FROM
    ${source_table}
WHERE
    year = ${year}
    AND month = ${month}
    AND day = ${day}
    AND webrequest_source = 'text'
    -- drop requests with no timestamps
    AND dt != '-'
    AND uri_path = '/beacon/impression'
    AND agent_type = 'user'
    AND uri_param_value('debug', uri_query) = 'false'
    -- sample_rate can be infinity, leading to Druid indexation failing.
    -- We remove those rows from the data
    AND cast(uri_param_value('recordImpressionSampleRate', uri_query) AS float) != 'Infinity'
    -- TODO: add once added to webrequest
    -- AND x_analytics_map['proxy'] IS NULL
GROUP BY
    CONCAT(SUBSTRING(dt, 0, 17), '00Z'),
    uri_param_value('campaign', uri_query),
    uri_param_value('banner', uri_query),
    uri_param_value('project', uri_query),
    uri_param_value('uselang', uri_query),
    uri_param_value('bucket', uri_query),
    uri_param_value('anonymous', uri_query) = 'true',
    uri_param_value('statusCode', uri_query),
    uri_param_value('country', uri_query),
    geocoded_data['country_code'] = uri_param_value('country', uri_query),
    geocoded_data['subdivision'],
    uri_param_value('device', uri_query),
    uri_param_value('recordImpressionSampleRate', uri_query)
;

DROP TABLE IF EXISTS tmp_banner_activity_${year}_${month}_${day};
