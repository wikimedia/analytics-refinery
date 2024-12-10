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
--     spark3-sql -f backfill_pageview_actor_2024_12.hql          \
--         -d refinery_hive_jar_path=hdfs:///wmf/refinery/current/artifacts/refinery-hive-shaded.jar \
--         -d source_table=wmf.pageview_actor                     \
--         -d actor_label_table=wmf.webrequest_actor_label_hourly \
--         -d destination_table=wmf.backfill_pageview_actor_2024_12 \
--         -d year=2024                                           \
--         -d month=7                                             \
--         -d day=5                                              \
--         -d hour=0                                              \
--         -d coalesce_partitions=32

ADD JAR ${refinery_hive_jar_path};
CREATE TEMPORARY FUNCTION get_pageview_info AS 'org.wikimedia.analytics.refinery.hive.GetPageviewInfoUDF';

WITH automated_actor AS (
    -- Enforce distinct to prevent potential traffic-row duplication in case of bug
    SELECT DISTINCT actor_signature_per_project_family as automated_actor_signature_per_project_family
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
        is_redirect_to_pageview,
        geocoded_data,
        user_agent_map,
        x_analytics_map,
        ts,
        access_method,
        CASE WHEN (agent_type IN ('user', 'automated') AND automated_actor_signature_per_project_family IS NOT NULL) THEN 'automated' ELSE agent_type END AS agent_type,
        referer_class,
        normalized_host,
        get_pageview_info(uri_host, uri_path, uri_query) AS pageview_info,
        page_id,
        namespace_id,
        actor_signature,
        actor_signature_per_project_family,
        referer_data
    FROM ${source_table}
        LEFT JOIN automated_actor
            ON actor_signature_per_project_family = automated_actor_signature_per_project_family
    WHERE year=${year} AND month=${month} AND day=${day} AND hour=${hour}
;
