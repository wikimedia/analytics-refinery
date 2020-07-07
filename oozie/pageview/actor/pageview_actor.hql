-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          extraction for.
--     actor_label_table -- Fully qualified table name containing actors label
--                          to join with source and flag automated traffic.
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
--     hive -f pageview_actor.hql                                \
--         -d refinery_hive_jar_path}=hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar \
--         -d source_table=wmf.webrequest                        \
--         -d actor_label_table=predictions.actor_label_hourly   \
--         -d destination_table=wmf.pageview_actor               \
--         -d year=2020                                          \
--         -d month=6                                            \
--         -d day=1                                              \
--         -d hour=1
--


-- Prevent hive from using a map-side join on the automated_actor CTE
-- as it regularly causes the following hive bug (the bug is non-deterministic):
-- https://issues.apache.org/jira/browse/HIVE-14555
SET hive.auto.convert.join           = false;

SET parquet.compression              = SNAPPY;
SET mapred.reduce.tasks              = 32;

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
    SELECT
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
        get_actor_signature(ip, user_agent, accept_language, normalized_host.project_class, uri_query, x_analytics_map) AS actor_signature_per_project_family
    FROM ${source_table}
        LEFT JOIN automated_actor
            ON get_actor_signature(ip, user_agent, accept_language, uri_host, uri_query, x_analytics_map) = automated_actor_signature
    WHERE webrequest_source IN ('text') AND
        year=${year} AND month=${month} AND day=${day} AND hour=${hour}
        AND (is_pageview = TRUE
          OR is_redirect_to_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics) = TRUE)
    -- Group by all fields (no aggregation as we have unique (hostname, sequence))
    -- to force a reduce step and have the expected number of output files
    GROUP BY
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
        is_redirect_to_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics),
        geocoded_data,
        user_agent_map,
        x_analytics_map,
        ts,
        access_method,
        CASE WHEN (agent_type = 'user' AND automated_actor_signature IS NOT NULL) THEN 'automated' ELSE agent_type END,
        referer_class,
        normalized_host,
        pageview_info,
        page_id,
        namespace_id,
        get_actor_signature(ip, user_agent, accept_language, uri_host, uri_query, x_analytics_map),
        get_actor_signature(ip, user_agent, accept_language, normalized_host.project_class, uri_query, x_analytics_map)
;
