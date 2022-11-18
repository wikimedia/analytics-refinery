-- Extracts one day of json formatted sampled webrequest to be loaded in Druid
--
-- Usage:
--     hive -f generate_daily_druid_webrequests.hql \
--         -d source_table=wmf.webrequest \
--         -d destination_directory=/wmf/tmp/druid/daily_json_webrequests \
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
    `cache_status`          string,
    `response_size`         string,
    `http_method`           string,
    `uri_host`              string,
    `uri_path`              string,
    `uri_query`             string,
    `content_type`          string,
    `referer`               string,
    `user_agent`            string,
    `client_port`           string,
    `x_cache`               string,
    `continent`             string,
    `country_code`          string,
    `isp`                   string,
    `as_number`             string,
    `is_pageview`           boolean,
    `is_debug`              boolean,
    `tls_version`           string,
    `tls_key_exchange`      string,
    `tls_auth`              string,
    `tls_cipher`            string,
    `is_from_public_cloud`  boolean,
    `requestctl`            string,
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
    cache_status,
    response_size,
    http_method,
    uri_host,
    uri_path,
    uri_query,
    content_type,
    referer,
    user_agent,
    x_analytics_map['client_port'] as client_port,
    x_cache,
    geocoded_data['continent'] as continent,
    geocoded_data['country_code'] as country_code,
    isp_data['isp'] as isp,
    isp_data['autonomous_system_number'] as as_number,
    is_pageview,
    coalesce(x_analytics_map['debug'], '0') = '1' as is_debug,
    tls_map['vers'] as tls_version,
    tls_map['keyx'] as tls_key_exchange,
    tls_map['auth'] as tls_auth,
    tls_map['ciph'] as tls_cipher,
    coalesce(x_analytics_map['public_cloud'], '0') = '1' as is_from_public_cloud,
    x_analytics_map['requestctl'] as requestctl,
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
    cache_status,
    response_size,
    http_method,
    uri_host,
    uri_path,
    uri_query,
    content_type,
    referer,
    user_agent,
    x_analytics_map['client_port'],
    x_cache,
    geocoded_data['continent'],
    geocoded_data['country_code'],
    isp_data['isp'],
    isp_data['autonomous_system_number'],
    is_pageview,
    coalesce(x_analytics_map['debug'], '0') = '1',
    tls_map['vers'],
    tls_map['keyx'],
    tls_map['auth'],
    tls_map['ciph'],
    x_analytics_map['requestctl'],
    coalesce(x_analytics_map['public_cloud'], '0') = '1'
;


DROP TABLE IF EXISTS tmp_daily_druid_webrequests_${year}_${month}_${day};
