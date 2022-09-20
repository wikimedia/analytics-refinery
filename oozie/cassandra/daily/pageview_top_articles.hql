-- Parameters:
--     refinery_hive_jar_path -- HDFS path to refinery hive jar
--     destination_directory  -- HDFS path to write output files
--     source_table           -- Fully qualified table name to compute from.
--     separator              -- Separator for values
--     year                   -- year of partition to compute from.
--     month                  -- month of partition to compute from.
--     day                    -- day of partition to compute from.
--
-- Usage:
--     hive -f pageview_top_articles.hql                          \
--         -d refinery_hive_jar_path=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/org/wikimedia/analytics/refinery/refinery-hive-X.X.X.jar \
--         -d destination_directory=/wmf/tmp/analytics/pageview_top_articles    \
--         -d source_table=wmf.pageview_hourly                    \
--         -d separator=\t                                        \
--         -d year=2015                                           \
--         -d month=5                                             \
--         -d day=1                                               \
--


ADD JAR ${refinery_hive_jar_path};

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


WITH ranked AS (
    SELECT
        project,
        page_title,
        access,
        year,
        month,
        day,
        views,
        rank() OVER (PARTITION BY project, access, year, month, day ORDER BY views DESC) as rank,
        row_number() OVER (PARTITION BY project, access, year, month, day ORDER BY views DESC) as rn
    FROM (
        SELECT
            project,
            reflect("org.json.simple.JSONObject", "escape", regexp_replace(page_title, '${separator}', '')) AS page_title,
            COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') AS access,
            LPAD(year, 4, "0") as year,
            LPAD(month, 2, "0") as month,
            LPAD(day, 2, "0") as day,
            SUM(view_count) as views
        FROM ${source_table}
        WHERE
            year = ${year}
            AND month = ${month}
            AND day = ${day}
            AND agent_type = 'user'
            -- Remove special unknown pageview (see T117346)
            AND page_title != '-'
        GROUP BY project, regexp_replace(page_title, '${separator}', ''), access_method, year, month, day
        GROUPING SETS (
            (project, regexp_replace(page_title, '${separator}', ''), access_method, year, month, day),
            (project, regexp_replace(page_title, '${separator}', ''), year, month, day)
        )
    ) raw
),
max_rank AS (
    SELECT
        project,
        access,
        year,
        month,
        day,
        rank as max_rank
    FROM ranked
    WHERE
        rn = 1001
    GROUP BY
        project,
        access,
        year,
        month,
        day,
        rank
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
        ranked.day,
        CONCAT('[',
            CONCAT_WS(',', collect_set(
                CONCAT('{"article":"', ranked.page_title,
                    '","views":', CAST(ranked.views AS STRING),
                    ',"rank":', CAST(ranked.rank AS STRING), '}'))
            ),']')
    )
FROM ranked
LEFT JOIN max_rank ON (
    ranked.project = max_rank.project
    AND ranked.access = max_rank.access
    AND ranked.year = max_rank.year
    AND ranked.month = max_rank.month
    AND ranked.day = max_rank.day
)
WHERE ranked.rank < COALESCE(max_rank.max_rank, 1001)
GROUP BY
    ranked.project,
    ranked.access,
    ranked.year,
    ranked.month,
    ranked.day
;
