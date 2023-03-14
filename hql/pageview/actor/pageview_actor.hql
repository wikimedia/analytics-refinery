-- Parameters:
--     refinery_hive_jar_path  -- The path to the refinery-hive jar to use for UDFs
--     source_table            -- Fully qualified table name to compute the
--                                extraction for.
--     actor_label_table       -- Fully qualified table name containing actors label
--                                to join with source and flag automated traffic.
--     destination_table       -- Fully qualified table name to fill in
--                                extracted values.
--     coalesce_partitions     -- The number of files to write per hour
--     year                    -- year of partition to compute aggregation for.
--     month                   -- month of partition to compute aggregation for.
--     day                     -- day of partition to compute aggregation for.
--     hour                    -- hour of partition to compute aggregation for.
--
-- Usage:
--     spark3-sql -f pageview_actor.hql                           \
--         -d refinery_hive_jar_path=hdfs:///wmf/refinery/current/artifacts/refinery-hive-shaded.jar \
--         -d source_table=wmf.webrequest                         \
--         -d actor_label_table=wmf.webrequest_actor_label_hourly \
--         -d destination_table=wmf.pageview_actor                \
--         -d year=2023                                           \
--         -d month=1                                             \
--         -d day=30                                              \
--         -d hour=0                                              \
--         -d coalesce_partitions=32

ADD JAR ${refinery_hive_jar_path};
CREATE TEMPORARY FUNCTION is_redirect_to_pageview as 'org.wikimedia.analytics.refinery.hive.IsRedirectToPageviewUDF';
CREATE TEMPORARY FUNCTION get_actor_signature AS 'org.wikimedia.analytics.refinery.hive.GetActorSignatureUDF';

WITH automated_actor AS (
    -- Enforce distinct to prevent potential traffic-row duplication in case of bug
    SELECT DISTINCT actor_signature as automated_actor_signature
    FROM ${actor_label_table}
    WHERE year=${year} AND month=${month} AND day=${day} AND hour=${hour}
        AND label = 'automated'
)

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        hostname,
        sequence,
        dt,
        time_firstbyte,
        ip,
        http_status,
        response_size,
        uri_host,
        uri_path,
        uri_query,
        content_type,
        referer,
        user_agent,
        accept_language,
        is_pageview,
        is_redirect_to_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics) AS is_redirect_to_pageview,
        geocoded_data,
        user_agent_map,
        x_analytics_map,
        ts,
        access_method,
        CASE WHEN (agent_type = 'user' AND automated_actor_signature IS NOT NULL) THEN 'automated' ELSE agent_type END AS agent_type,
        referer_class,
        normalized_host,
        pageview_info,
        page_id,
        namespace_id,
        get_actor_signature(ip, user_agent, accept_language, uri_host, uri_query, x_analytics_map) AS actor_signature,
        get_actor_signature(ip, user_agent, accept_language, normalized_host.project_class, uri_query, x_analytics_map) AS actor_signature_per_project_family,
        referer_data
    FROM ${source_table}
        LEFT JOIN automated_actor
            ON get_actor_signature(ip, user_agent, accept_language, uri_host, uri_query, x_analytics_map) = automated_actor_signature
    WHERE webrequest_source IN ('text') AND
        year=${year} AND month=${month} AND day=${day} AND hour=${hour}
        AND (is_pageview = TRUE
          OR is_redirect_to_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics) = TRUE)
;
