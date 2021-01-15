-- Generates mobile apps uniques daily - iOS and Android Installation
--
-- Value set is generated using a CTE to extract uuids from the current day
-- ${source_table}, and then computing distinct uniques count for this data set.
-- Those values are finally concatenated to previously computed data available in
-- ${archive_table}.
-- This dataset is inserted in a temporary external table which format is TSV
-- The end of the oozie job then moves this file to the archive table directory,
-- overwriting the exisiting file.
--
-- Parameters:
--     source_table      -- table containing source data
--     archive_table     -- Fully qualified table name where
--                          to find archived data.
--     temporary_directory
--                       -- Temporary directory to store computed data
--     year              -- year of the to-be-generated
--     month             -- month of the to-be-generated
--     day               -- day of the to-be-generated
--
--
-- Usage:
--     hive -f generate_uniques_daily.hql
--         -d source_table=wmf.webrequest
--         -d archive_table=wmf.mobile_apps_uniques_daily
--         -d temporary_directory=/wmf/tmp/analytics/mobile_apps/2015-2-1
--         -d year=2015
--         -d month=2
--         -d day=1
--

-- Set compression codec to gzip to provide asked format
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


-- Create a temporary table, then compute the new unique count
-- and concatenate it to archived data.
DROP TABLE IF EXISTS tmp_mobile_apps_uniques_${year}_${month}_${day};
CREATE EXTERNAL TABLE tmp_mobile_apps_uniques_${year}_${month}_${day} (
    `year`                 int     COMMENT 'Unpadded year of request',
    `month`                int     COMMENT 'Unpadded month of request',
    `day`                  int     COMMENT 'Unpadded day of request',
    `platform`             string  COMMENT 'Mobile platform from user agent parsing',
    `unique_count`         bigint  COMMENT 'Distinct uuid count'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${temporary_directory}'
;

WITH mobile_apps_uuids_${year}_${month}_${day} AS
(
    SELECT
        year,
        month,
        day,
        CASE WHEN (user_agent LIKE '%iOS%' OR user_agent LIKE '%iPhone%') THEN 'iOS' ELSE 'Android' END AS platform,
        COALESCE(x_analytics_map['wmfuuid'],
                 parse_url(concat('http://bla.org/woo/', uri_query), 'QUERY', 'appInstallID')) AS uuid
    FROM ${source_table}
    WHERE user_agent LIKE('WikipediaApp%')
        AND ((parse_url(concat('http://bla.org/woo/', uri_query), 'QUERY', 'action') = 'mobileview' AND uri_path == '/w/api.php')
            OR (uri_path LIKE '/api/rest_v1%' AND uri_query == ''))
        AND COALESCE(x_analytics_map['wmfuuid'],
                     parse_url(concat('http://bla.org/woo/', uri_query), 'QUERY', 'appInstallID')) IS NOT NULL
        AND webrequest_source IN ('text')
        AND year=${year}
        AND month=${month}
        AND day=${day}
)
INSERT OVERWRITE TABLE tmp_mobile_apps_uniques_${year}_${month}_${day}
SELECT
    year,
    month,
    day,
    platform,
    unique_count
FROM
    (
        SELECT
            year,
            month,
            day,
            platform,
            unique_count
        FROM
            ${archive_table}
        WHERE NOT ((year = ${year})
            AND (month = ${month})
            AND (day = ${day}))

        UNION ALL

        SELECT
            year,
            month,
            day,
            platform,
            COUNT(DISTINCT uuid) AS unique_count
        FROM
            mobile_apps_uuids_${year}_${month}_${day}
        GROUP BY
            year,
            month,
            day,
            platform
    ) old_union_new_uniques_daily
ORDER BY
    year,
    month,
    day,
    platform
-- Limit enforced by hive strict mapreduce setting.
-- 1000000000 == NO LIMIT !
LIMIT 1000000000
;

-- Drop temporary table (not needed anymore with hive 0.14)
DROP TABLE IF EXISTS tmp_mobile_apps_uniques_${year}_${month}_${day};
