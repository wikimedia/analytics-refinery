-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          extraction for.
--     destination_table -- Fully qualified table name to fill in
--                          extracted values.
--     year              -- year of partition to compute aggregation
--                          for.
--     month             -- month of partition to compute aggregation
--                          for.
--     day               -- day of partition to compute aggregation
--                          for.
--     hour              -- hour of partition to compute aggregation
--                          for.
--
-- Usage:
--     hive -f wdqs_extract.hql                                   \
--         -d source_table=wmf.webrequest                         \
--         -d destination_table=wmf.wdqs_extract                  \
--         -d year=2016                                           \
--         -d month=9                                             \
--         -d day=1                                               \
--         -d hour=0
--

SET parquet.compression              = SNAPPY;
SET mapred.reduce.tasks              = 8;

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
    SELECT
        hostname,
        sequence,
        dt,
        time_firstbyte,
        ip,
        cache_status,
        http_status,
        response_size,
        http_method,
        uri_host,
        uri_path,
        uri_query,
        content_type,
        referer,
        x_forwarded_for,
        user_agent,
        accept_language,
        x_analytics,
        range,
        is_pageview,
        record_version,
        client_ip,
        geocoded_data,
        x_cache,
        user_agent_map,
        x_analytics_map,
        ts,
        access_method,
        agent_type,
        is_zero,
        referer_class,
        normalized_host,
        pageview_info,
        page_id
    FROM
        ${source_table}
    WHERE webrequest_source = 'misc'
        AND year = ${year} AND month = ${month} AND day = ${day} AND hour = ${hour}
        AND uri_host = 'query.wikidata.org'
    GROUP BY
        hostname,
        sequence,
        dt,
        time_firstbyte,
        ip,
        cache_status,
        http_status,
        response_size,
        http_method,
        uri_host,
        uri_path,
        uri_query,
        content_type,
        referer,
        x_forwarded_for,
        user_agent,
        accept_language,
        x_analytics,
        range,
        is_pageview,
        record_version,
        client_ip,
        geocoded_data,
        x_cache,
        user_agent_map,
        x_analytics_map,
        ts,
        access_method,
        agent_type,
        is_zero,
        referer_class,
        normalized_host,
        pageview_info,
        page_id
;
