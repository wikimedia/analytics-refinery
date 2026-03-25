-- Parameters:
--     source_table        -- Fully qualified table name to compute the
--                            aggregation from.
--     destination_table   -- Fully qualified table name to write to
--     year                -- year of partition to compute statistics
--                            for.
--     month               -- month of partition to compute statistics
--                            for.
--     day                 -- day of partition to compute statistics
--                            for.
--     hour                -- hour of partition to compute statistics
--                            for.
--     coalesce_partitions -- the number of final partitions.
--
-- Usage:
--     spark3-sql --master yarn -f aggregate_pageview_to_fundraising_projectview.hql    \
--         -d source_table=wmf.pageview_hourly                                          \
--         -d destination_table=wmf.projectview_aggregate_for_fundraising               \
--         -d year=2025                                                                 \
--         -d month=9                                                                   \
--         -d day=1                                                                     \
--         -d hour=1                                                                    \
--         -d coalesce_partitions=1
--
-- NOTE: for backfilling, use this query, and make sure to keep the select clause in order

-- SET spark.sql.sources.partitionOverwriteMode=dynamic;
-- INSERT OVERWRITE TABLE wmf.projectview_aggregate_for_fundraising
--     SELECT /*+ COALESCE(1) */
--         project,
--         language_variant,
--         user_agent_map['browser_family'] as browser_family,
--         user_agent_map['os_family'] as os_family,
--         access_method,
--         agent_type,
--         country_code,
--         country,
--         referer_class,
--         referer_name,
--         SUM(view_count) AS view_count,
--         year, month, day, hour
--     FROM
--         wmf.pageview_hourly
--     WHERE
--         year=2025 and month=9 and day=1 and hour in (1, 2)
--     GROUP BY
--         project,
--         language_variant,
--         user_agent_map['browser_family'],
--         user_agent_map['os_family'],
--         access_method,
--         agent_type,
--         country_code,
--         country,
--         referer_class,
--         referer_name,
--         year, month, day, hour

SET parquet.compression = SNAPPY;

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        project,
        language_variant,
        user_agent_map['browser_family'] as browser_family,
        user_agent_map['os_family'] as os_family,
        access_method,
        agent_type,
        country_code,
        country,
        referer_class,
        referer_name,

        SUM(view_count) AS view_count
    FROM
        ${source_table}
    WHERE year=${year} AND month=${month} AND day=${day} AND hour=${hour}
    GROUP BY
        project,
        language_variant,
        user_agent_map['browser_family'],
        user_agent_map['os_family'],
        access_method,
        agent_type,
        country_code,
        country,
        referer_class,
        referer_name
