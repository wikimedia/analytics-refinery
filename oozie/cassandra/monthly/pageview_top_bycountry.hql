-- Parameters:
--     destination_directory -- HDFS path to write output files
--     source_table          -- Fully qualified table name to compute from.
--     separator             -- Separator for values
--     year                  -- year of partition to compute from.
--     month                 -- month of partition to compute from.
--     day                   -- day of partition to compute from.
--
-- Usage:
--     hive -f pageview_top_bycountry.hql                         \
--         -d destination_directory=/wmf/tmp/analytics/pageview_top_bycountry   \
--         -d source_table=wmf.projectview_hourly                 \
--         -d separator='\t'                                        \
--         -d year=2015                                           \
--         -d month=5                                             \
--


SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

WITH ranked AS (
    SELECT
        project,
        access,
        country,
        year,
        month,
        views,
        rank() OVER (PARTITION BY project, access, year, month ORDER BY raw_views DESC) as rank,
        row_number() OVER (PARTITION BY project, access, year, month ORDER BY raw_views DESC) as rn
    FROM (
        SELECT
            COALESCE(project, 'all-projects') AS project,
            COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') AS access,
            country_code as country,
            LPAD(year, 4, "0") as year,
            LPAD(month, 2, "0") as month,
            SUM(view_count) as raw_views,
            CEIL(SUM(view_count) / 1000) * 1000 as views
        FROM ${source_table}
        WHERE
            year = ${year}
            AND month = ${month}
            AND agent_type = 'user'
        GROUP BY project, access_method, country_code, year, month
        GROUPING SETS (
            (project, access_method, country_code, year, month),
            (project, country_code, year, month),
            (access_method, country_code, year, month),
            (country_code, year, month)
        )
        HAVING SUM(view_count) > 99
    ) raw
)
INSERT OVERWRITE DIRECTORY "${destination_directory}"
-- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
-- works for exports to local directories (see HIVE-5672), we have to
-- prepare the lines by hand through concatenation :-(
SELECT
    CONCAT_WS("${separator}",
        ranked.project,
        ranked.access,
        ranked.year,
        ranked.month,
        CONCAT('[',
            CONCAT_WS(',', collect_set(
                CONCAT('{"country":"', ranked.country,
                    '","views":', CAST(ranked.views AS STRING),
                    ',"rank":', CAST(ranked.rank AS STRING), '}'))
            ),']')
    )
FROM ranked
GROUP BY
    ranked.project,
    ranked.access,
    ranked.year,
    ranked.month
;