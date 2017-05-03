-- Extracts one day of json formatted daily uniques to be loaded in Druid
--
-- Usage:
--     hive -f generate_daily_druid_uniques.hql \
--         -d source_table=wmf.last_access_uniques_daily \
--         -d destination_directory=/tmp/druid/daily_json_uniques \
--         -d year=2016 \
--         -d month=7 \
--         -d day=10
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS tmp_daily_druid_uniques_${year}_${month}_${day};


CREATE EXTERNAL TABLE IF NOT EXISTS tmp_daily_druid_uniques_${year}_${month}_${day} (
    `dt`                     string,
    `host`                   string,
    `country`                string,
    `country_code`           string,
    `uniques_underestimate`  bigint,
    `uniques_offset`         bigint,
    `uniques_estimate`       bigint
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';


WITH filtered_hosts AS (
    SELECT
        uri_host as filtered_host,
        SUM(uniques_estimate) as checked_uniques_estimate
    FROM ${source_table}
    WHERE year=${year}
      AND month=${month}
      AND day=${day}
    GROUP BY
        uri_host
    HAVING
        SUM(uniques_estimate) >= 1000
)

INSERT OVERWRITE TABLE tmp_daily_druid_uniques_${year}_${month}_${day}
SELECT
    CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-',
        LPAD(day, 2, '0'), 'T00:00:00Z') AS dt,
    uri_host AS host,
    country AS country,
    country_code AS country_code,
    uniques_underestimate as uniques_underestimate,
    uniques_offset AS uniques_offset,
    uniques_estimate AS uniques_estimate
FROM ${source_table}
    INNER JOIN filtered_hosts
        ON uri_host = filtered_host
WHERE year = ${year}
    AND month = ${month}
    AND day = ${day};


DROP TABLE IF EXISTS tmp_daily_druid_uniques_${year}_${month}_${day};
