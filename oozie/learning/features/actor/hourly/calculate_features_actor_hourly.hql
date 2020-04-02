-- Computing hourly features per actor
-- to use them  later to label automated traffic
-- Companion doc: https://docs.google.com/document/d/1q14GH7LklhMvDh0jwGaFD4eXvtQ5tLDmw3UeFTmb3KM/edit

-- Parameters:
--     refinery_hive_jar_path -- The path to the refinery-hive jar to use for UDFs
--     version                -- The version of the features we gather
--     source_table           -- Fully qualified table name to compute the
--                               aggregation from.
--     destination_table      -- Fully qualified table name to fill in
--                               aggregated values.
--     year                   -- year of partition to compute aggregation
--                               for.
--     month                  -- month of partition to compute aggregation
--                               for.
--     day                    -- day of partition to compute aggregation
--                               for.
--     hour                   -- hour of partition to compute aggregation
--                               for.
--
-- Usage:
--     hive -f calculate_features_actor_hourly.hql     \
--         -d refinery_hive_jar_path=hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar  \
--         -d version = 0.1                            \
--         -d source_table=wmf.webrequest              \
--         -d destination_table=features.actor_hourly  \
--         -d year=2015                                \
--         -d month=6                                  \
--         -d day=1                                    \
--         -d hour=1
--

SET parquet.compression              = SNAPPY;
SET mapred.reduce.tasks              = 8;

ADD JAR ${refinery_hive_jar_path};
CREATE TEMPORARY FUNCTION get_actor_signature AS 'org.wikimedia.analytics.refinery.hive.GetActorSignatureUDF';

WITH hourly_actor_data as (
    SELECT
        ts,
        ip,
        lower(uri_host) as domain,
        get_actor_signature(ip, user_agent, accept_language, uri_host, uri_query, x_analytics_map) AS actor_signature,
        http_status,
        user_agent,
        x_analytics_map["nocookies"] as nocookies,
        pageview_info['page_title'] as page_title
    FROM
        ${source_table}
    WHERE webrequest_source IN ('text')
        AND year=${year}
        AND month=${month}
        AND day=${day}
        AND hour=${hour}
        AND is_pageview = 1
        AND agent_type = "user"
        # weblight data is a mess, there is no x-forwarded-for and all looks like it comes from the same IP
        AND user_agent not like "%weblight%"
        AND COALESCE(pageview_info['project'], '') != ''
)


INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})

    SELECT
        ${version} as version,
        actor_signature as actor_signature,
        min(ts) as first_interaction_dt,
        max(ts) as last_interaction_dt,
        count(*) as pageview_count,
        cast((count(*)/(unix_timestamp(max(ts)) - unix_timestamp( min(ts))) * 60) as int) as pageview_rate_per_min,
        sum(coalesce(nocookies, 0L)) as nocookies,
        MAX(length(user_agent)) as user_agent_length,
        COUNT(DISTINCT page_title) as distinct_pages_visited_count
    FROM
        hourly_actor_data
    GROUP BY
        actor_signature;



