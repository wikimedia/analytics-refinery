-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          aggregation from.
--     destination_table -- Fully qualified table name to fill with
--                          aggregates.
--     record_version    -- record_version keeping track of changes
--                          in the table content definition.
--     year              -- year of partition to compute statistics
--                          for.
--     month             -- month of partition to compute statistics
--                          for.
--     day               -- day of partition to compute statistics
--                          for.
--     hour              -- hour of partition to compute statistics
--                          for.
--
-- Usage:
--     hive -f aggregate_pageview_to_projectview.hql              \
--         -d source_table=wmf.pageview_hourly                    \
--         -d destination_table=wmf.projectview_hourly            \
--         -d record_version=0.0.1                                \
--         -d year=2015                                           \
--         -d month=6                                             \
--         -d day=1                                               \
--         -d hour=1
--

SET parquet.compression              = SNAPPY;
SET mapred.reduce.tasks              = 1;


INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
    SELECT
        project,
        access_method,
        NULL AS zero_carrier,
        agent_type,
        referer_class,
        continent,
        country_code,
        '${record_version}' AS record_version,
        SUM(view_count) AS view_count
    FROM
        ${source_table}
    WHERE year=${year} AND month=${month} AND day=${day} AND hour=${hour}
    GROUP BY
        project,
        access_method,
        agent_type,
        referer_class,
        continent,
        country_code
;
