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
--     spark3-sql --master yarn -f compute_pageview_hourly.hql \
--         -d source_table=wmf_traffic.webrequest_v2 \
--         -d destination_table=milimetric.pageview_hourly \
--         -d record_version=0.0.1 \
--         -d year=2023 \
--         -d month=2 \
--         -d day=7 \
--         -d hour=1 \
--         -d coalesce_partitions=8
--

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        target.wiki AS project,
        target.language_variant AS language_variant,
        target.page_title AS page_title,
        access_method,
        NULL as zero_carrier,
        CASE
            -- Format the agent_type using the legacy bot detection split.
            WHEN agent_type = 'human' THEN 'user'
            WHEN agent_type = 'bot' AND bot_detection.is_bot_user_agent THEN 'spider'
            ELSE 'automated'
        END AS agent_type,
        request.referer_type AS referer_class,
        geo.continent AS continent,
        geo.country_code AS country_code,
        geo.country AS country,
        geo.subdivision AS subdivision,
        geo.city AS city,
        -- have to destructure and restructure the map because it's not
        -- orderable so Spark can't use it in a GROUP BY
        map(
            'device_family', client.device_family,
            'browser_family', client.browser_family,
            'browser_major', client.browser_major,
            'os_family', client.os_family,
            'os_major', client.os_major,
            'os_minor', client.os_minor,
            'wmf_app_version', client.app_version
        ),
        '${record_version}' AS record_version,
        COUNT(1) AS view_count,
        target.page_id,
        target.namespace_id,
        request.referer_name AS referer_name
    FROM ${source_table}
    WHERE
        dt >= make_timestamp(${year}, ${month}, ${day}, ${hour}, 0, 0)
        AND dt < make_timestamp(${year}, ${month}, ${day}, ${hour}, 0, 0) + INTERVAL '1' HOUR
        AND request_type = 'pageview'
        AND COALESCE(target.wiki, '') != ''
    GROUP BY
        target.wiki,
        target.language_variant,
        target.page_title,
        access_method,
        CASE
            WHEN agent_type = 'human' THEN 'user'
            WHEN agent_type = 'bot' AND bot_detection.is_bot_user_agent THEN 'spider'
            ELSE 'automated'
        END,
        request.referer_type,
        geo.continent,
        geo.country_code,
        -- this is going to cause a split in case MaxMind renames a country.  However,
        -- we may find this split useful in understanding how data changes across these
        -- renames.  There's an argument for keeping this grouping and just grouping
        -- by country_code in any downstream queries.
        geo.country,
        geo.subdivision,
        geo.city,
        client.device_family,
        client.browser_family,
        client.browser_major,
        client.os_family,
        client.os_major,
        client.os_minor,
        client.app_version,
        target.page_id,
        target.namespace_id,
        request.referer_name
;
