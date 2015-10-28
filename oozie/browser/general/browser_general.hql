
-- Parameters:
--     projectview_source       -- Table containing hourly projectviews.
--     pageview_source          -- Table containing hourly pageviews.
--     destination_directory    -- Directory where to write the report.
--     access_methods           -- Comma-separated list of access methods
--                                 to include: 'xxx', 'yyy', ...
--     year                     -- Year of interval's start date.
--     month                    -- Month of interval's start date.
--     day                      -- Day of interval's start date.
--     time_window              -- Time window to compute in days. The end
--                                 date will be calculated adding this to
--                                 the start date. Start date is included
--                                 in the report, but end date is not.
--
-- Usage:
--     hive -f browser_general.hql                             \
--         -d projectview_source=wmf.projectview_hourly        \
--         -d pageview_source=wmf.pageview_hourly              \
--         -d destination_directory=/tmp/foo                   \
--         -d access_methods='desktop'                         \
--         -d year=2015                                        \
--         -d month=10                                         \
--         -d day=11                                           \
--         -d time_window=7
--

-- Permits cartesian join of small enough table.
SET hive.mapred.mode = nonstrict;

SET start_date = CONCAT('${year}-', LPAD(${month}, 2, '0'), '-', LPAD(${day}, 2, '0'));
SET end_date   = DATE_ADD(${hiveconf:start_date}, ${time_window});

WITH total AS (
    SELECT
        SUM(view_count) as view_count_total
    FROM
        ${projectview_source}
    WHERE
        CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) >= ${hiveconf:start_date}
        AND CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) < ${hiveconf:end_date}
        AND agent_type = 'user'
        AND access_method IN (${access_methods})
)
INSERT OVERWRITE DIRECTORY '${destination_directory}'
    SELECT
        tsv_line
    FROM (
        SELECT
            -- Since "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" only
            -- works for hive>=1.2.0 (see HIVE-5672), we have to prepare the
            -- lines by hand through concatenation.
            CONCAT_WS(
                '\t',
                CONCAT(user_agent_map['os_family'], ' ', user_agent_map['os_major']),
                CONCAT(user_agent_map['browser_family'], ' ', user_agent_map['browser_major']),
                CAST(ROUND(SUM(view_count) * 100 / total.view_count_total, 2) AS string)
            ) AS tsv_line,
            SUM(view_count) * 100 / total.view_count_total AS percent
        FROM
            ${pageview_source}
            JOIN total
        WHERE
            CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) >= ${hiveconf:start_date}
            AND CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) < ${hiveconf:end_date}
            AND agent_type = 'user'
            AND access_method IN (${access_methods})
        GROUP BY
            CONCAT(user_agent_map['os_family'], ' ', user_agent_map['os_major']),
            CONCAT(user_agent_map['browser_family'], ' ', user_agent_map['browser_major']),
            total.view_count_total
        HAVING
            (SUM(view_count) * 100 / total.view_count_total) > 0.1
        ORDER BY percent DESC
    ) AS tsv_lines
;
