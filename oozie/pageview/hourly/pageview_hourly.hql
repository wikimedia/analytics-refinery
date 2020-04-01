-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          aggregation for.
--     actor_label_table -- Fully qualified table name containing actors label
--                          to join with source and flag automated traffic
--     destination_table -- Fully qualified table name to fill in
--                          aggregated values.
--     record_version    -- record_version keeping track of changes
--                          in the table content definition.
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
--     hive -f pageview_hourly.hql                               \
--         -d ${refinery_hive_jar_path}=hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar \
--         -d source_table=wmf.webrequest                        \
--         -d actor_label_table=predictions.actor_label_hourly   \
--         -d destination_table=wmf.pageview_hourly              \
--         -d record_version=0.0.1                               \
--         -d year=2015                                          \
--         -d month=6                                            \
--         -d day=1                                              \
--         -d hour=1
--

SET parquet.compression              = SNAPPY;
SET mapred.reduce.tasks              = 8;

-- The join by actor makes some instances of the reduce-phase
-- require more memory (twice a month for 2020-01).
-- Given there is only 8 reducers, this bump will go un-noticed
-- from the global resource consumption side.
SET mapreduce.reduce.memory.mb=6144;
SET mapreduce.reduce.java.opts=-Xmx4916m;

ADD JAR ${refinery_hive_jar_path};
CREATE TEMPORARY FUNCTION get_actor_signature AS 'org.wikimedia.analytics.refinery.hive.GetActorSignatureUDF';

WITH automated_actor AS (
    -- Enforce distinct to prevent potential traffic-row duplication in case of bug
    SELECT DISTINCT actor_signature
    FROM ${actor_label_table}
    WHERE year=${year} AND month=${month} AND day=${day} AND hour=${hour}
        AND label = 'automated'
)

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
    SELECT
        pageview_info['project'] AS project,
        pageview_info['language_variant'] AS language_variant,
        pageview_info['page_title'] AS page_title,
        access_method,
        NULL as zero_carrier,
        CASE WHEN (agent_type = 'user' AND actor_signature IS NOT NULL) THEN 'automated' ELSE agent_type END AS agent_type,
        referer_class,
        geocoded_data['continent'] AS continent,
        geocoded_data['country_code'] AS country_code,
        geocoded_data['country'] AS country,
        geocoded_data['subdivision'] AS subdivision,
        geocoded_data['city'] AS city,
        user_agent_map,
        '${record_version}' AS record_version,
        COUNT(1) AS view_count,
        page_id,
        namespace_id
    FROM ${source_table}
        LEFT JOIN automated_actor
            ON get_actor_signature(ip, user_agent, accept_language, uri_host, uri_query, x_analytics_map) = actor_signature
    WHERE webrequest_source IN ('text') AND
        year=${year} AND month=${month} AND day=${day} AND hour=${hour}
        AND is_pageview = TRUE
        AND COALESCE(pageview_info['project'], '') != ''
    GROUP BY
        pageview_info['project'],
        pageview_info['language_variant'],
        pageview_info['page_title'],
        access_method,
        CASE WHEN (agent_type = 'user' AND actor_signature IS NOT NULL) THEN 'automated' ELSE agent_type END,
        referer_class,
        geocoded_data['continent'],
        geocoded_data['country_code'],
        geocoded_data['country'],
        geocoded_data['subdivision'],
        geocoded_data['city'],
        user_agent_map,
        page_id,
        namespace_id
;
