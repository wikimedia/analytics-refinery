-- Extracts one day of json formatted daily per-domain unique devices to be loaded in Druid
--
-- Usage:
--     hive -f generate_druid_unique_devices_per_domain_daily.hql \
--         -d source_table=wmf.unique_devices_per_domain_daily \
--         -d destination_directory=/wmf/tmp/druid/unique_devices_per_domain_daily_json \
--         -d year=2016 \
--         -d month=7 \
--         -d day=10
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS tmp_druid_unique_devices_per_domain_daily_${year}_${month}_${day};


CREATE EXTERNAL TABLE IF NOT EXISTS tmp_druid_unique_devices_per_domain_daily_${year}_${month}_${day} (
    `dt`                     string,
    `domain`                 string,
    `country`                string,
    `country_code`           string,
    `uniques_underestimate`  bigint,
    `uniques_offset`         bigint,
    `uniques_estimate`       bigint
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';


WITH filtered_domains AS (
    SELECT
        domain AS filtered_domain,
        SUM(uniques_estimate) AS checked_uniques_estimate
    FROM ${source_table}
    WHERE year=${year}
      AND month=${month}
      AND day=${day}
    GROUP BY
        domain
    HAVING
        SUM(uniques_estimate) >= 1000
)

INSERT OVERWRITE TABLE tmp_druid_unique_devices_per_domain_daily_${year}_${month}_${day}
SELECT
    CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-',
        LPAD(day, 2, '0'), 'T00:00:00Z') AS dt,
    domain AS domain,
    country AS country,
    country_code AS country_code,
    uniques_underestimate AS uniques_underestimate,
    uniques_offset AS uniques_offset,
    uniques_estimate AS uniques_estimate
FROM ${source_table}
    INNER JOIN filtered_domains
        ON domain = filtered_domain
WHERE year = ${year}
    AND month = ${month}
    AND day = ${day};


DROP TABLE IF EXISTS tmp_druid_unique_devices_per_domain_daily_${year}_${month}_${day};
