-- Extracts one hour of formatted sampled webrequest to be loaded in Druid
--
-- Usage:
--     spark-sql -f generate_hourly_druid_webrequest_sampled.hql \
--         -d source_table=wmf_raw.webrequest_sampled \
--         -d destination_table=tmp_hourly_druid_webrequest_sampled_2026_01_01_00 \
--         -d destination_directory=/wmf/tmp/druid/tmp_hourly_druid_webrequest_sampled_2026_01_01_00 \
--         -d coalesce_partitions=4 \
--         -d year=2026 \
--         -d month=1 \
--         -d day=1 \
--         -d hour=0
--


DROP TABLE IF EXISTS ${destination_table};

CREATE TABLE IF NOT EXISTS ${destination_table} (
    accept                    string,
    accept_language           string,
    as_number                 bigint,
    authorization             string,
    backend                   string,
    cache_status              string,
    content_type              string,
    continent                 string,
    country_code              string,
    dt                        string,
    hostname                  string,
    http_method               string,
    http_status               string,
    https                     string,
    ip                        string,
    is_debug                  string,
    is_from_public_cloud      string,
    is_pageview               string,
    isp                       string,
    ja3n                      string,
    ja4h                      string,
    nocookies                 string,
    range                     string,
    referer                   string,
    requestctl                string,
    res_proxy                 string,
    response_size             bigint,
    sequence                  bigint,
    server_pid                string,
    termination_state         string,
    time_firstbyte            double,
    tls_auth                  string,
    tls_cipher                string,
    tls_key_exchange          string,
    tls_sess                  string,
    tls_version               string,
    uri_host                  string,
    uri_path                  string,
    uri_query                 string,
    user_agent                string,
    webrequest_source         string,
    wmfuniq_days              string,
    wmfuniq_freq              string,
    wmfuniq_weeks             string,
    x_cache                   string,
    x_is_browser              string,

    -- Fields to be used as metricsas HiveToDruid
    -- uses the same names for Hive fields and druid metrics
    aggregated_response_size  bigint,
    aggregated_time_firstbyte double,
    hits                      bigint
)
USING PARQUET
OPTIONS ('compression'='gzip')
LOCATION '${destination_directory}';

WITH prepared_data AS (
    SELECT DISTINCT
        accept,
        accept_language,
        as_number,
        authorization,
        backend,
        cache_status,
        content_type,
        continent,
        country_code,
        dt,
        hostname,
        http_method,
        http_status,
        https,
        ip,
        is_debug,
        is_from_public_cloud,
        is_pageview,
        isp,
        ja3n,
        ja4h,
        nocookies,
        range,
        referer,
        requestctl,
        res_proxy,
        response_size,
        sequence,
        server_pid,
        termination_state,
        time_firstbyte,
        tls_auth,
        tls_cipher,
        tls_key_exchange,
        tls_sess,
        tls_version,
        uri_host,
        uri_path,
        uri_query,
        user_agent,
        webrequest_source,
        wmfuniq_days,
        wmfuniq_freq,
        wmfuniq_weeks,
        x_cache,
        x_is_browser,
        -- Special fields
        response_size AS aggregated_response_size,
        time_firstbyte AS aggregated_time_firstbyte,
        1 AS hits
    FROM ${source_table}
    WHERE year = ${year}
        AND month = ${month}
        AND day = ${day}
        AND hour = ${hour}
        -- Druid doesn't accept null timestamps
        AND dt IS NOT NULL
        AND dt != '-'
)
INSERT OVERWRITE TABLE ${destination_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */ *
FROM prepared_data;
