-- Parameters:
--     refinery_hive_jar_path  -- HDFS path to refinery hive jar
--     destination_directory   -- HDFS path to write output files
--     source_table            -- Fully qualified table name to compute from
--     country_blacklist_table -- Fully qualified table name containing the countries that should be excluded from the results
--     separator               -- Separator for values
--     year                    -- year of partition to compute from
--     month                   -- month of partition to compute from
--     day                     -- day of partition to compute from
--
-- Usage:
--     hive -f pageview_top_percountry.hql \
--         -d refinery_hive_jar_path=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/org/wikimedia/analytics/refinery/refinery-hive-0.1.1.jar \
--         -d destination_directory=/tmp/pageview_top_percountry \
--         -d source_table=wmf.pageview_actor \
--         -d country_blacklist_table=wmf.geoeditors_blacklist_country \
--         -d separator=\t \
--         -d year=2021 \
--         -d month=1 \
--         -d day=1
--

ADD JAR ${refinery_hive_jar_path};

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

-- This is needed to prevent a HiveRelDecorrelator.decorrelateRel error for the grouping sets
SET hive.cbo.enable=false;
-- This is needed to allow a Cartesian product between the source and country blacklist tables
SET hive.mapred.mode=nonstrict;

WITH base_data AS (
    SELECT
        geocoded_data['country_code'] AS country_code,
        REGEXP_REPLACE(access_method, ' ', '-') AS access,
        pageview_info['project'] AS project,
        REFLECT('org.json.simple.JSONObject', 'escape', REGEXP_REPLACE(pageview_info['page_title'], '${separator}', '')) AS page_title,
        LPAD(year, 4, '0') as year,
        LPAD(month, 2, '0') as month,
        LPAD(day, 2, '0') as day,
        actor_signature
    FROM ${source_table} source
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
        AND agent_type = 'user'
        AND pageview_info IS NOT NULL
        AND geocoded_data IS NOT NULL
        AND is_pageview
        -- Remove special unknown pageview (see T117346)
        AND pageview_info['page_title'] != '-'
        AND geocoded_data['country_code'] != '--'
        -- This NOT EXISTS executes as a map join, and was chosen after benchmarking against LEFT JOIN and NOT IN
        AND NOT EXISTS (
            SELECT 1
            FROM ${country_blacklist_table} country_blacklist
            WHERE country_blacklist.country_code = source.geocoded_data['country_code']
        )
),
raw AS (
    SELECT
        country_code,
        COALESCE(access, 'all-access') AS access,
        project,
        page_title,
        year,
        month,
        day,
        COUNT(1) AS total_view_count,
        COUNT(DISTINCT actor_signature) AS unique_actor_count
    FROM base_data
    GROUP BY
        country_code,
        access,
        project,
        page_title,
        year,
        month,
        day
    GROUPING SETS (
        (
            country_code,
            access,
            project,
            page_title,
            year,
            month,
            day
        ),
        (
            country_code,
            project,
            page_title,
            year,
            month,
            day
        )
    )
),
ranked AS (
    SELECT
        country_code,
        access,
        project,
        page_title,
        year,
        month,
        day,
        CEIL(total_view_count / 100) * 100 AS views_ceil,
        rank() OVER (PARTITION BY access, country_code, year, month, day ORDER BY total_view_count DESC) as rank,
        row_number() OVER (PARTITION BY access, country_code, year, month, day ORDER BY total_view_count DESC) as rn
    FROM raw
    WHERE unique_actor_count > 1000
),
max_rank AS (
    SELECT
        country_code,
        access,
        year,
        month,
        day,
        rank as max_rank
    FROM ranked
    WHERE rn = 1001
    GROUP BY
        country_code,
        access,
        year,
        month,
        day,
        rank
)
INSERT OVERWRITE DIRECTORY '${destination_directory}'
-- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
-- works for exports to local directories (see HIVE-5672), we have to
-- prepare the lines by hand through concatenation :-(
SELECT
    CONCAT_WS(
        '${separator}',
        ranked.country_code,
        ranked.access,
        ranked.year,
        ranked.month,
        ranked.day,
        CONCAT(
            '[',
            CONCAT_WS(
                ',',
                COLLECT_SET(
                    CONCAT(
                        '{\"article\":\"',
                        ranked.page_title,
                        '\",\"project\":\"',
                        ranked.project,
                        '\",\"views_ceil\":',
                        CAST(ranked.views_ceil AS STRING),
                        ',\"rank\":',
                        CAST(ranked.rank AS STRING),
                        '}'
                    )
                )
            ),
            ']'
        )
    )
FROM ranked
LEFT JOIN max_rank ON (
    ranked.country_code = max_rank.country_code
    AND ranked.access = max_rank.access
    AND ranked.year = max_rank.year
    AND ranked.month = max_rank.month
    AND ranked.day = max_rank.day
)
WHERE
    ranked.rank < COALESCE(max_rank.max_rank, 1001)
GROUP BY
    ranked.country_code,
    ranked.access,
    ranked.year,
    ranked.month,
    ranked.day
;