-- Generates mobile apps uniques monthly - iOS and Android Installation.
--
-- Value set is generated using a CTE to extract uuids from the current month
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
--
--
-- Usage:
--     hive -f generate_uniques_monthly.hql
--         -d source_table=wmf.webrequest
--         -d archive_table=wmf.mobile_apps_uniques_monthly
--         -d temporary_directory=/wmf/tmp/analytics/mobile_apps/2015-2
--         -d year=2015
--         -d month=2


-- Set compression codec to gzip to provide asked format
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

-- Allocate resources to almost all maps before starting reducers
SET mapreduce.job.reduce.slowstart.completedmaps=0.99;


-- Create a temporary table, then compute the new unique count
-- and concatenate it to archived data.
DROP TABLE IF EXISTS tmp_mobile_apps_uniques_${year}_${month};
CREATE EXTERNAL TABLE tmp_mobile_apps_uniques_${year}_${month} (
    `year`                 int     COMMENT 'Unpadded year of request',
    `month`                int     COMMENT 'Unpadded month of request',
    `platform`             string  COMMENT 'Mobile platform from user agent parsing',
    `unique_count`         bigint  COMMENT 'Distinct uuid count'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${temporary_directory}'
;

WITH mobile_apps_uuids_${year}_${month} AS
(
    SELECT
        year,
        month,
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
)
INSERT OVERWRITE TABLE tmp_mobile_apps_uniques_${year}_${month}
SELECT
    year,
    month,
    platform,
    unique_count
FROM
    (
        SELECT
            year,
            month,
            platform,
            unique_count
        FROM
            ${archive_table}
        WHERE NOT ((year = ${year})
            AND (month = ${month}))

        UNION ALL

        SELECT
            year,
            month,
            platform,
            COUNT(DISTINCT uuid) AS unique_count
        FROM
            mobile_apps_uuids_${year}_${month}
        GROUP BY
            year,
            month,
            platform
    ) old_union_new_uniques_monthly
ORDER BY
    year,
    month,
    platform
-- Limit enforced by hive strict mapreduce setting.
-- 1000000000 == NO LIMIT !
LIMIT 1000000000
;

-- Drop temporary table (not needed anymore with hive 0.14)
DROP TABLE IF EXISTS tmp_mobile_apps_uniques_${year}_${month};
