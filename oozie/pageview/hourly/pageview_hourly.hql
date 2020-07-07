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
--
-- Usage:
--     hive -f pageview_hourly.hql                               \
--         -d source_table=wmf.pageview_actor                    \
--         -d destination_table=wmf.pageview_hourly              \
--         -d record_version=0.0.1                               \
--         -d year=2015                                          \
--         -d month=6                                            \
--         -d day=1                                              \
--         -d hour=1
--

SET parquet.compression              = SNAPPY;
SET mapred.reduce.tasks              = 8;

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
    SELECT
        pageview_info['project'] AS project,
        pageview_info['language_variant'] AS language_variant,
        pageview_info['page_title'] AS page_title,
        access_method,
        NULL as zero_carrier,
        agent_type,
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
    WHERE year=${year} AND month=${month} AND day=${day} AND hour=${hour}
        AND is_pageview = TRUE
        AND COALESCE(pageview_info['project'], '') != ''
    GROUP BY
        pageview_info['project'],
        pageview_info['language_variant'],
        pageview_info['page_title'],
        access_method,
        agent_type,
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
