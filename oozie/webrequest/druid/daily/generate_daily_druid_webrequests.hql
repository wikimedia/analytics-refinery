-- Extracts one day of json formatted sampled webrequest to be loaded in Druid
--
-- Usage:
--     hive -f generate_daily_druid_webrequests.hql \
--         -d source_table=wmf.webrequest \
--         -d destination_directory=/tmp/druid/daily_json_webrequests \
--         -d year=2016 \
--         -d month=7 \
--         -d day=10
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS tmp_daily_druid_webrequests_${year}_${month}_${day};


CREATE EXTERNAL TABLE IF NOT EXISTS tmp_daily_druid_webrequests_${year}_${month}_${day} (
    `dt`                    string,
    `webrequest_source`     string,
    `hostname`              string,
    `time_firstbyte`        string,
    `ip`                    string,
    `http_status`           string,
    `response_size`         string,
    `http_method`           string,
    `uri_host`              string,
    `uri_path`              string,
    `uri_query`             string,
    `content_type`          string,
    `referer`               string,
    `user_agent`            string,
    `x_cache`               string,
    `hits`                  bigint
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';


INSERT OVERWRITE TABLE tmp_daily_druid_webrequests_${year}_${month}_${day}
SELECT
    dt,
    webrequest_source,
    hostname,
    time_firstbyte,
    ip,
    http_status,
    response_size,
    http_method,
    uri_host,
    uri_path,
    uri_query,
    content_type,
    referer,
    user_agent,
    x_cache,
    count(1) as hits
FROM ${source_table}
  TABLESAMPLE(BUCKET 1 OUT OF 128 ON hostname, sequence)
WHERE year = ${year}
    AND month = ${month}
    AND day = ${day}
    AND dt IS NOT NULL
    AND dt != '-'
GROUP BY
    dt,
    webrequest_source,
    hostname,
    time_firstbyte,
    ip,
    http_status,
    response_size,
    http_method,
    uri_host,
    uri_path,
    uri_query,
    content_type,
    referer,
    user_agent,
    x_cache
;


DROP TABLE IF EXISTS tmp_daily_druid_webrequests_${year}_${month}_${day};
