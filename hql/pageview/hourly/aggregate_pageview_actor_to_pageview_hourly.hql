-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          aggregation for.
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
--     coalesce_partitions -- the number of final partitions.
--
-- Usage:
--     spark3-sql --master yarn -f aggregate_pageview_actor_to_pageview_hourly.hql  \
--         -d source_table=wmf.pageview_actor                                       \
--         -d destination_table=milimetric.pageview_hourly                          \
--         -d record_version=0.0.1                                                  \
--         -d year=2023                                                             \
--         -d month=2                                                               \
--         -d day=7                                                                 \
--         -d hour=1                                                                \
--         -d coalesce_partitions=8
--

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        pageview_info['project'] AS project,
        pageview_info['language_variant'] AS language_variant,
        pageview_info['page_title'] AS page_title,
        access_method,
        NULL as zero_carrier,
        agent_type,
        referer_data.referer_class AS referer_class,
        geocoded_data['continent'] AS continent,
        geocoded_data['country_code'] AS country_code,
        geocoded_data['country'] AS country,
        geocoded_data['subdivision'] AS subdivision,
        geocoded_data['city'] AS city,
        -- have to destructure and restructure the map because it's not
        -- orderable so Spark can't use it in a GROUP BY
        map_from_entries(array(
            struct('device_family',   user_agent_map['device_family']),
            struct('browser_family',  user_agent_map['browser_family']),
            struct('browser_major',   user_agent_map['browser_major']),
            struct('os_family',       user_agent_map['os_family']),
            struct('os_major',        user_agent_map['os_major']),
            struct('os_minor',        user_agent_map['os_minor']),
            struct('wmf_app_version', user_agent_map['wmf_app_version'])
        )),
        '${record_version}' AS record_version,
        COUNT(1) AS view_count,
        page_id,
        namespace_id,
        referer_data.referer_name AS referer_name
    FROM ${source_table}
    WHERE year=${year} AND month=${month} AND day=${day} AND hour=${hour}
        AND is_pageview = TRUE
        AND COALESCE(pageview_info['project'], '') != ''
    GROUP BY
        pageview_info['project'],
        pageview_info['language_variant'],
        pageview_info['page_title'],
        access_method,
        agent_type,
        referer_data.referer_class,
        geocoded_data['continent'],
        geocoded_data['country_code'],
        -- this is going to cause a split in case MaxMind renames a country.  However,
        -- we may find this split useful in understanding how data changes across these
        -- renames.  There's an argument for keeping this grouping and just grouping
        -- by country_code in any downstream queries.
        geocoded_data['country'],
        geocoded_data['subdivision'],
        geocoded_data['city'],
        user_agent_map['device_family'],
        user_agent_map['browser_family'],
        user_agent_map['browser_major'],
        user_agent_map['os_family'],
        user_agent_map['os_major'],
        user_agent_map['os_minor'],
        user_agent_map['wmf_app_version'],
        page_id,
        namespace_id,
        referer_data.referer_name
;
