-- Computing hourly features per actor
-- to use them  later to label automated traffic
-- Companion doc: https://docs.google.com/document/d/1q14GH7LklhMvDh0jwGaFD4eXvtQ5tLDmw3UeFTmb3KM/edit

-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          aggregation from.
--     destination_table -- Fully qualified table name to fill in
--                          aggregated values.
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
--     hive -f calculate_features_actor_hourly.hql                                \
--         -d source_table=wmf.webrequest                                           \
--         -d destination_table=features.actor_hourly                    \
--         -d year=2015                                                             \
--         -d month=6                                                               \
--         -d day=1                                                                 \
--         -d hour=1
--

SET parquet.compression              = SNAPPY;
SET mapred.reduce.tasks              = 8;

WITH hourly_actor_data as (
    SELECT
        ts,
        ip,
        lower(uri_host) as domain,
        md5(concat(ip, substr(user_agent,0,200), accept_language, uri_host, COALESCE(x_analytics_map['wmfuuid'],parse_url(concat('http://bla.org/woo/', uri_query), 'QUERY', 'appInstallID'),''))) AS actor_id,
        http_status,
        user_agent,
        x_analytics_map["nocookies"] as nocookies
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
        actor_id as actor_id,
        max(ts) as interaction_start_ts,
        min(ts) as interaction_end_ts,
        (unix_timestamp(max(ts)) - unix_timestamp( min(ts))) as interaction_length_secs,
        count(*) as pageview_count,
        cast((count(*)/(unix_timestamp(max(ts)) - unix_timestamp( min(ts))) * 60) as int) as pageview_ratio_per_min,
        sum(coalesce(nocookies, 0L)) as nocookies,
        length(user_agent) as user_agent_length
    FROM
        hourly_actor_data
    GROUP BY
        actor_id, user_agent;



