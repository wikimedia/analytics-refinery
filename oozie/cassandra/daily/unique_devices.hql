-- Parameters:
--     destination_directory -- HDFS path to write output files
--     source_table          -- Fully qualified table name to compute from.
--     separator             -- Separator for values
--     year                  -- year of partition to compute from.
--     month                 -- month of partition to compute from.
--     day                   -- day of partition to compute from.
--
-- Usage:
--     hive -f unique_devices.hql                                 \
--         -d destination_directory=/tmp/unique_devices           \
--         -d source_table=wmf.last_access_uniques_daily          \
--         -d separator=\t                                        \
--         -d year=2016                                           \
--         -d month=1                                             \
--         -d day=1                                               \
--


SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


WITH unique_devices AS (
    SELECT
        CONCAT(
            regexp_extract(uri_host, '^((?!www)([a-z0-9-_]+)\\.)(m\\.)?\\w+\\.org$'),
            regexp_extract(uri_host, '([a-z0-9-_]+)\\.org$')
            ) AS project,
        CASE WHEN uri_host RLIKE '(^(m)\\.)|\\.m\\.'
            THEN 'mobile-site'
            ELSE 'desktop-site'
            END AS access_site,
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0")) AS dt,
        SUM(uniques_estimate) AS devices
    FROM
        ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
    GROUP BY
        CONCAT(
            regexp_extract(uri_host, '^((?!www)([a-z0-9-_]+)\\.)(m\\.)?\\w+\\.org$'),
            regexp_extract(uri_host, '([a-z0-9-_]+)\\.org$')
            ),
        CASE WHEN uri_host RLIKE '(^(m)\\.)|\\.m\\.'
            THEN 'mobile-site'
            ELSE 'desktop-site'
            END,
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0"))
    HAVING SUM(uniques_estimate) > 1000
)

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    SELECT
        CONCAT_WS('${separator}',
            project,
            COALESCE(access_site, 'all-sites'),
            dt,
            CAST(SUM(devices) AS STRING)) as line
    FROM
        unique_devices
    GROUP BY
        project,
        access_site,
        dt
    GROUPING SETS (
        (
            project,
            access_site,
            dt
        ),(
            project,
            dt
        )
    );
