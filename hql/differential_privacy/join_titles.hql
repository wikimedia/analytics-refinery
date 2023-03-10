-- Parameters:
--     source_table          -- Fully qualified table name to compute the
--                              transformation from.
--     destination_directory -- Directory where to write transformation
--                              results
--     year                  -- year of partition to compute statistics for.
--     month                 -- month of partition to compute statistics for.
--     day                   -- day of partition to compute statistics for.
--     hour                  -- hour of partition to compute statistics for.
--
-- Usage:
--     spark3-sql --master yarn -f join_titles.hql                                                \
--         -d source_table=differential_privacy.country_language_page_eps_1_delta_1e_07_2023_2_10 \
--         -d destination_directory=/wmf/tmp/differential_privacy/example                         \
--         -d year=2023                                                                           \
--         -d month=2                                                                             \
--         -d day=10

SET spark.hadoop.hive.exec.compress.output=false;

WITH titles AS (
    SELECT
        project,
        page_title,
        page_id
    FROM
        wmf.pageview_hourly
    WHERE
        agent_type <> 'spider'
        AND project is not null
        AND page_title != '-'
        AND year = ${year}
        AND month = ${month}
        AND day = ${day}
    GROUP BY project, page_title, page_id
    HAVING SUM(view_count) >= 150
),

joined AS (
    SELECT
        dp.*,
        t.page_title,
        ROW_NUMBER() OVER (
            PARTITION BY dp.country, dp.project, dp.page_id
            ORDER BY t.page_title ASC
        ) AS row_num
    FROM ${source_table} dp
    JOIN titles t
    ON
        dp.project = t.project
        AND cast(dp.page_id as int) = t.page_id
)

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Set 0 as volume column since we don't use it.
    USING csv
    OPTIONS ('compression' 'uncompressed', 'sep' ' ')

    SELECT
        country,
        project,
        page_id,
        page_title,
        gbc
    FROM joined
    WHERE row_num = 1
;

